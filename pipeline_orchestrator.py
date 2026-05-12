#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Konflux Pipeline Test Orchestrator
=====================================
Automates the full pipeline-test lifecycle:
  1. Validate cluster connectivity (oc access)
  2. Trigger builds via ansible-playbook
  3. Watch pipeline runs until all reach terminal state
  4. Extract metrics immediately to results/<run_id>/
  5. Append to results/registry_<user_id>.csv   ? per-user, no git conflicts
  6. Optionally git commit + push the results

Git-based collaboration model
------------------------------
Results live inside the repo under results/.  Every user writes only to their
own registry file, so two people pushing at the same time never conflict.

  # Your daily workflow:
  git pull                                          # get colleagues' latest
  python3 pipeline_orchestrator.py \\
      --starts-from 1 --ends-to 10 \\
      --test-scenario rok --git-push               # auto-commits and pushes
  python3 compare_runs.py --all                    # reads all registry_*.csv
  git add results/reports/ && git commit && git push   # share the report

Config file (JSON) - set once per user, never pass flags again:
  ~/.konflux_test.json  or  ./pipeline_test_config.json  (local wins over global)

  {
    "user_id":       "smodak",
    "oc_context":    "default/api-stone-stg-rh01-l2vh-p1-openshiftapps-com:6443/smodak",
    "ns_pattern":    "test-rhtap-[0-9]+-tenant",
    "app_label":     "example-packages",
    "poll_interval": 30,
    "timeout":       3600,
    "git_name":      "Subrata Modak",
    "git_email":     "smodak@redhat.com"
  }

Results layout:
  results/
    registry_<user_id>.csv          ? owned by you alone (no conflicts)
    <run_id>/
        run_config.json
        pipelineruns_raw.json        ? excluded from git (large; see .gitignore)
        pipeline_summary.csv
        taskrun_detail.csv
        run_summary.txt
    reports/
        comparison_<timestamp>.html  ? shareable via git
"""

import argparse
import csv
import datetime
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

# ?? Schema version - bump when adding/removing registry columns ???????????????
SCHEMA_VERSION = "1.1"

# ?? Defaults ??????????????????????????????????????????????????????????????????
DEFAULT_OC_CONTEXT  = "default/api-stone-stg-rh01-l2vh-p1-openshiftapps-com:6443/smodak"
DEFAULT_NS_PATTERN  = r"test-rhtap-[0-9]+-tenant"
DEFAULT_APP_LABEL   = "example-packages"
DEFAULT_POLL_SEC    = 15
DEFAULT_TIMEOUT_SEC = 3600

RESULTS_DIR = Path(__file__).parent / "results"

# Per-user registry lives at results/registry_<user_id>.csv
# This ensures two collaborators pushing simultaneously never conflict.
REGISTRY_FIELDS = [
    "run_id", "schema_version", "user_id",
    "batch_size", "starts_from", "ends_to", "test_scenario",
    "date_utc", "start_utc", "end_utc", "trigger_duration_sec",
    "total_pipelines", "succeeded", "failed", "still_running",
    "avg_duration_sec", "p50_duration_sec", "p90_duration_sec",
    "min_duration_sec", "max_duration_sec",
    "oc_context", "ns_pattern", "app_label",
    "result_dir", "run_status",
]

TERMINAL_STATES = {"Succeeded", "Failed", "Cancelled"}


# ?? Config file loading ????????????????????????????????????????????????????????

def load_config(explicit_path: str | None) -> dict:
    """Load config from explicit path ? ./pipeline_test_config.json ? ~/.konflux_test.json."""
    candidates = []
    if explicit_path:
        candidates.append(Path(explicit_path))
    candidates.append(Path(__file__).parent / "pipeline_test_config.json")
    candidates.append(Path.home() / ".konflux_test.json")
    for path in candidates:
        if path.exists():
            try:
                cfg = json.loads(path.read_text())
                log(f"Config loaded from: {path}")
                return cfg
            except json.JSONDecodeError as e:
                log(f"Config file {path} is invalid JSON: {e}", "WARN")
    return {}


def resolve_config(args) -> dict:
    """Merge config file defaults with CLI flags (CLI always wins)."""
    cfg = load_config(args.config)
    def pick(cli_val, key, default):
        return cli_val if cli_val is not None else cfg.get(key, default)
    return {
        "user_id":       pick(args.user_id,       "user_id",       os.environ.get("USER", "user")),
        "oc_context":    pick(args.oc_context,    "oc_context",    DEFAULT_OC_CONTEXT),
        "ns_pattern":    pick(args.ns_pattern,    "ns_pattern",    DEFAULT_NS_PATTERN),
        "app_label":     pick(args.app_label,     "app_label",     DEFAULT_APP_LABEL),
        "poll_interval": pick(args.poll_interval, "poll_interval", DEFAULT_POLL_SEC),
        "timeout":       pick(args.timeout,       "timeout",       DEFAULT_TIMEOUT_SEC),
        "git_name":      cfg.get("git_name",  ""),
        "git_email":     cfg.get("git_email", ""),
    }


# ?? CLI ????????????????????????????????????????????????????????????????????????

def parse_args():
    p = argparse.ArgumentParser(
        description="Konflux pipeline test orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--starts-from",   type=int, required=True,
                   help="First repo/component index to trigger (e.g. 1)")
    p.add_argument("--ends-to",       type=int, required=True,
                   help="Last repo/component index to trigger (e.g. 10). "
                        "Total pipelines = ends_to - starts_from + 1")
    p.add_argument("--test-scenario", default="rok",
                   help="Test scenario passed to ansible-playbook: rok or fkx (default: rok)")

    p.add_argument("--user-id",    default=None,
                   help="Your Kerberos/GitHub username (default: $USER env var). "
                        "Used as prefix in run_id and registry filename.")
    p.add_argument("--config",     default=None,
                   help="Path to JSON config file. Falls back to "
                        "./pipeline_test_config.json then ~/.konflux_test.json")

    p.add_argument("--oc-context", default=None,
                   help="oc --context value for the target cluster "
                        "(overrides config file)")
    p.add_argument("--ns-pattern", default=None,
                   help="Regex to match tenant namespaces "
                        "(default: test-rhtap-[0-9]+-tenant)")
    p.add_argument("--app-label",  default=None,
                   help="appstudio.openshift.io/application label to filter pipeline runs "
                        "(default: example-packages)")
    p.add_argument("--poll-interval", type=int, default=None,
                   help="Seconds between status-check polls (default: 15)")
    p.add_argument("--timeout",       type=int, default=None,
                   help="Max seconds to wait for all pipelines before extracting "
                        "partial results (default: 3600)")

    p.add_argument("--git-commit", action="store_true",
                   help="git commit results after extraction (with Signed-off-by trailer)")
    p.add_argument("--git-push",   action="store_true",
                   help="git push after commit (implies --git-commit)")

    p.add_argument("--dry-run",      action="store_true",
                   help="Validate cluster and print plan, but do NOT trigger or extract")
    p.add_argument("--skip-trigger", action="store_true",
                   help="Skip ansible-playbook trigger; watch+extract only. "
                        "Looks back 2 hours for already-running/completed runs. "
                        "Exits early if no runs found after 5 consecutive empty polls.")
    return p.parse_args()


# ?? Utilities ??????????????????????????????????????????????????????????????????

_last_was_progress = False


def log(msg: str, level: str = "INFO"):
    global _last_was_progress
    if _last_was_progress:
        print(flush=True)
        _last_was_progress = False
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts} UTC] [{level}] {msg}", flush=True)


def progress(msg: str):
    global _last_was_progress
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S")
    line = f"[{ts} UTC] [INFO] {msg}"
    if sys.stdout.isatty():
        try:
            import shutil as _sh
            width = _sh.get_terminal_size(fallback=(160, 40)).columns
        except Exception:
            width = 160
        print(f"\r{line[:width-1].ljust(width-1)}", end="", flush=True)
        _last_was_progress = True
    else:
        print(line, flush=True)


def progress_done():
    global _last_was_progress
    if _last_was_progress:
        print(flush=True)
        _last_was_progress = False


def utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)


def utcnow_str() -> str:
    return utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def local_tz_name() -> str:
    try:
        import time as _t
        return _t.tzname[_t.daylight] or "UTC"
    except Exception:
        return "unknown"


def run_oc(args_list: list, context: str,
           timeout: int = 60) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(
            ["oc", "--context", context] + args_list,
            capture_output=True, text=True, timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        log(f"oc {' '.join(args_list[:2])} timed out after {timeout}s", "WARN")
        return subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr="timeout")


def duration_str(seconds) -> str:
    try:
        m, s = divmod(int(seconds), 60)
        return f"{m}m{s:02d}s"
    except (ValueError, TypeError):
        return ""


def percentile(lst: list, pct: float):
    if not lst:
        return None
    return lst[max(0, int(len(lst) * pct) - 1)]


def parse_k8s_time(ts: str) -> datetime.datetime | None:
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ"):
        try:
            return datetime.datetime.strptime(ts, fmt)
        except (ValueError, TypeError):
            continue
    return None


def classify_run(pr: dict) -> str:
    conds = pr.get("status", {}).get("conditions", [])
    if not conds:
        return "Running"
    reason = conds[-1].get("reason", "")
    if reason == "Succeeded":
        return "Succeeded"
    if reason in ("Failed", "CouldntGetTask", "TaskRunImagePullFailed",
                  "ExceededResourceQuota", "PipelineRunTimeout", "StoppedRunFinally"):
        return "Failed"
    if reason in ("Cancelled", "PipelineRunCancelled"):
        return "Cancelled"
    return "Running"


# ?? Step 1: Cluster validation ?????????????????????????????????????????????????

def validate_cluster(context: str) -> bool:
    log("Validating cluster connectivity...")
    r = run_oc(["whoami"], context)
    if r.returncode != 0:
        log(f"oc whoami failed: {r.stderr.strip()}", "ERROR")
        log("Log in first: oclogin stone-stg-rh01", "ERROR")
        return False
    log(f"Authenticated as: {r.stdout.strip()}")
    r2 = run_oc(["get", "namespaces", "--no-headers", "-o", "name"], context)
    if r2.returncode != 0:
        log(f"Cannot list namespaces: {r2.stderr.strip()}", "ERROR")
        return False
    log(f"Cluster reachable - {len(r2.stdout.strip().splitlines())} namespaces visible")
    return True


# ?? Step 2: Trigger builds ?????????????????????????????????????????????????????

def trigger_builds(starts_from: int, ends_to: int, scenario: str) -> datetime.datetime:
    count = ends_to - starts_from + 1
    log(f"Triggering {count} pipeline(s): starts_from={starts_from} ends_at={ends_to} "
        f"test_scenario={scenario}")
    cmd = [
        "ansible-playbook", "playbooks/create-trigger-build-pipeline.yaml",
        "-e", f"starts_from={starts_from}",
        "-e", f"ends_at={ends_to}",
        "-e", f"test_scenario={scenario}",
    ]
    trigger_time = utcnow()
    log(f"Trigger time (UTC): {trigger_time.strftime('%Y-%m-%dT%H:%M:%SZ')}")
    result = subprocess.run(cmd, text=True)
    if result.returncode != 0:
        log("ansible-playbook exited non-zero - may have partially failed.", "WARN")
    else:
        log("ansible-playbook completed successfully")
    return trigger_time


# ?? Step 3: Watch pipeline runs ????????????????????????????????????????????????

def fetch_pipeline_runs(context, ns_pattern, app_label, since) -> list:
    r = run_oc(["get", "pipelinerun", "--all-namespaces", "-o", "json"], context)
    if r.returncode != 0:
        log(f"oc get pipelinerun failed: {r.stderr.strip()}", "WARN")
        return []
    try:
        data = json.loads(r.stdout)
    except json.JSONDecodeError:
        log("Failed to parse pipelinerun JSON", "WARN")
        return []

    matching = []
    for pr in data.get("items", []):
        ns = pr["metadata"]["namespace"]
        if not re.search(ns_pattern, ns):
            continue
        labels = pr["metadata"].get("labels", {})
        if app_label and labels.get("appstudio.openshift.io/application") != app_label:
            continue
        creation = parse_k8s_time(pr["metadata"].get("creationTimestamp", ""))
        if creation and creation < (since - datetime.timedelta(seconds=30)):
            continue
        matching.append(pr)
    return matching


def watch_pipeline_runs(context, ns_pattern, app_label, trigger_time,
                        expected, poll_sec, timeout_sec) -> list:
    """
    Poll until all expected runs reach a terminal state.

    GC-resilience: seen_runs cache ensures a run is never dropped once
    observed, even if Konflux prunes it between polls.

    Early-exit: if no runs have appeared after MAX_EMPTY_POLLS consecutive
    empty polls we assume GC beat us and bail out rather than waiting for
    the full timeout.
    """
    MAX_EMPTY_POLLS = 5
    empty_polls = 0

    log(f"Watching for {expected} pipeline run(s) -- poll every {poll_sec}s "
        f"(timeout {timeout_sec // 60}m)")
    start = time.time()

    # key: "<namespace>/<name>"  value: latest pipelinerun dict
    # Entries are never removed -- survives PAC GC pruning between polls.
    seen_runs: dict = {}

    while True:
        elapsed = int(time.time() - start)
        current = fetch_pipeline_runs(context, ns_pattern, app_label, trigger_time)

        for pr in current:
            key = f"{pr['metadata']['namespace']}/{pr['metadata']['name']}"
            seen_runs[key] = pr

        if not seen_runs:
            empty_polls += 1
            if empty_polls >= MAX_EMPTY_POLLS:
                progress_done()
                log(f"No pipeline runs found after {empty_polls} polls "
                    f"({empty_polls * poll_sec}s). Runs may have been "
                    f"GC-ed before collection. Exiting.", "WARN")
                return []
        else:
            empty_polls = 0

        all_runs = list(seen_runs.values())
        terminal = [r for r in all_runs if classify_run(r) in TERMINAL_STATES]
        running  = [r for r in all_runs if classify_run(r) not in TERMINAL_STATES]
        succ = sum(1 for r in terminal if classify_run(r) == "Succeeded")
        fail = len(terminal) - succ

        gc_pruned = len(all_runs) - len(current)
        gc_note = (f" [{gc_pruned} GC-pruned, held in cache]"
                   if gc_pruned > 0 else "")

        progress(f"Progress: {len(terminal)}/{expected} terminal "
                 f"({succ} succeeded, {fail} failed, {len(running)} running)"
                 f" -- {elapsed // 60}m{elapsed % 60:02d}s elapsed{gc_note}")

        if len(terminal) >= expected:
            progress_done()
            log("All expected pipeline runs reached terminal state.")
            return all_runs
        if elapsed >= timeout_sec:
            progress_done()
            log(f"Timeout after {timeout_sec}s -- extracting partial results.", "WARN")
            return all_runs

        time.sleep(poll_sec)


# ?? Step 4: Extract results ????????????????????????????????????????????????????

def extract_results(runs: list, run_dir: Path, run_config: dict) -> dict:
    run_dir.mkdir(parents=True, exist_ok=True)

    # Raw JSON (large - excluded from git via .gitignore)
    (run_dir / "pipelineruns_raw.json").write_text(json.dumps({"items": runs}, indent=2))

    # Pipeline summary CSV
    pipeline_rows = []
    for pr in runs:
        ns     = pr["metadata"]["namespace"]
        name   = pr["metadata"]["name"]
        labels = pr["metadata"].get("labels", {})
        state  = classify_run(pr)
        conds  = pr.get("status", {}).get("conditions", [])
        msg    = conds[-1].get("message", "")[:300] if conds else ""
        s_str  = pr.get("status", {}).get("startTime", "")
        e_str  = pr.get("status", {}).get("completionTime", "")
        dur    = ""
        if s_str and e_str:
            sv, ev = parse_k8s_time(s_str), parse_k8s_time(e_str)
            if sv and ev:
                dur = int((ev - sv).total_seconds())
        pipeline_rows.append({
            "schema_version": SCHEMA_VERSION,
            "run_id":         run_config["run_id"],
            "user_id":        run_config["user_id"],
            "namespace":      ns,
            "pipeline_run":   name,
            "application":    labels.get("appstudio.openshift.io/application", ""),
            "component":      labels.get("appstudio.openshift.io/component", ""),
            "status":         state,
            "start_time_utc": s_str,
            "end_time_utc":   e_str,
            "duration_sec":   dur,
            "duration_hms":   duration_str(dur),
            "failure_msg":    msg,
        })

    if pipeline_rows:
        with open(run_dir / "pipeline_summary.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=pipeline_rows[0].keys())
            w.writeheader()
            w.writerows(pipeline_rows)

    # TaskRun detail CSV
    taskrun_rows = []
    for pr in runs:
        pr_name = pr["metadata"]["name"]
        ns      = pr["metadata"]["namespace"]
        for ref in pr.get("status", {}).get("childReferences", []):
            tr_s = ref.get("status", {})
            s_str, e_str = tr_s.get("startTime", ""), tr_s.get("completionTime", "")
            dur = ""
            if s_str and e_str:
                sv, ev = parse_k8s_time(s_str), parse_k8s_time(e_str)
                if sv and ev:
                    dur = int((ev - sv).total_seconds())
            conds = tr_s.get("conditions", [{}])
            taskrun_rows.append({
                "schema_version": SCHEMA_VERSION,
                "run_id":    run_config["run_id"],
                "user_id":   run_config["user_id"],
                "pipeline_run": pr_name, "namespace": ns,
                "taskrun_name": ref.get("name", ""),
                "task_name":   ref.get("displayName", ref.get("pipelineTaskName", "")),
                "status":      conds[-1].get("reason", "") if conds else "",
                "start_time_utc": s_str, "end_time_utc": e_str, "duration_sec": dur,
            })
        for tr_name, tr_val in pr.get("status", {}).get("taskRuns", {}).items():
            ts    = tr_val.get("status", {})
            s_str = ts.get("startTime", "")
            e_str = ts.get("completionTime", "")
            dur   = ""
            if s_str and e_str:
                sv, ev = parse_k8s_time(s_str), parse_k8s_time(e_str)
                if sv and ev:
                    dur = int((ev - sv).total_seconds())
            conds = ts.get("conditions", [])
            taskrun_rows.append({
                "schema_version": SCHEMA_VERSION,
                "run_id":    run_config["run_id"],
                "user_id":   run_config["user_id"],
                "pipeline_run": pr_name, "namespace": ns,
                "taskrun_name": tr_name,
                "task_name":   tr_val.get("pipelineTaskName", ""),
                "status":      conds[-1].get("reason", "") if conds else "",
                "start_time_utc": s_str, "end_time_utc": e_str, "duration_sec": dur,
            })
    if taskrun_rows:
        with open(run_dir / "taskrun_detail.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=taskrun_rows[0].keys())
            w.writeheader()
            w.writerows(taskrun_rows)

    # Stats
    succeeded = [r for r in pipeline_rows if r["status"] == "Succeeded"]
    failed    = [r for r in pipeline_rows if r["status"] in ("Failed", "Cancelled")]
    running   = [r for r in pipeline_rows if r["status"] not in TERMINAL_STATES]
    durations = sorted([r["duration_sec"] for r in succeeded
                        if isinstance(r["duration_sec"], int)])
    stats = {
        "total":         len(pipeline_rows),
        "succeeded":     len(succeeded),
        "failed":        len(failed),
        "still_running": len(running),
        "avg_sec":  int(sum(durations) / len(durations)) if durations else "",
        "p50_sec":  percentile(durations, 0.50) or "",
        "p90_sec":  percentile(durations, 0.90) or "",
        "min_sec":  min(durations) if durations else "",
        "max_sec":  max(durations) if durations else "",
    }

    summary = "\n".join(filter(None, [
        f"=== Konflux Pipeline Run Summary ===",
        f"Run ID:          {run_config['run_id']}",
        f"User:            {run_config['user_id']}",
        f"Schema version:  {SCHEMA_VERSION}",
        f"Batch size:      {run_config['batch_size']}",
        f"Test scenario:   {run_config['test_scenario']}",
        f"Triggered (UTC): {run_config['start_utc']}",
        f"Completed (UTC): {run_config.get('end_utc', 'N/A')}",
        f"",
        f"Total pipelines: {stats['total']}",
        f"  Succeeded:     {stats['succeeded']}",
        f"  Failed:        {stats['failed']}",
        f"  Still running: {stats['still_running']}",
        f"",
        (f"Duration (succeeded pipelines):\n"
         f"  Avg: {duration_str(stats['avg_sec'])}\n"
         f"  P50: {duration_str(stats['p50_sec'])}\n"
         f"  P90: {duration_str(stats['p90_sec'])}\n"
         f"  Min: {duration_str(stats['min_sec'])}\n"
         f"  Max: {duration_str(stats['max_sec'])}"
         if durations else "No successful pipelines to compute durations."),
    ]))
    (run_dir / "run_summary.txt").write_text(summary)
    print("\n" + summary)
    return stats


# ?? Step 5: Update per-user registry ??????????????????????????????????????????

def append_registry(run_config: dict, stats: dict, run_dir: Path, run_status: str) -> Path:
    """Append to results/registry_<user_id>.csv - one file per user, no git conflicts."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    registry_file = RESULTS_DIR / f"registry_{run_config['user_id']}.csv"
    row = {
        "run_id":               run_config["run_id"],
        "schema_version":       SCHEMA_VERSION,
        "user_id":              run_config["user_id"],
        "batch_size":           run_config["batch_size"],
        "starts_from":          run_config["starts_from"],
        "ends_to":              run_config["ends_to"],
        "test_scenario":        run_config["test_scenario"],
        "date_utc":             run_config["date_utc"],
        "start_utc":            run_config["start_utc"],
        "end_utc":              run_config.get("end_utc", ""),
        "trigger_duration_sec": run_config.get("trigger_duration_sec", ""),
        "total_pipelines":      stats.get("total", ""),
        "succeeded":            stats.get("succeeded", ""),
        "failed":               stats.get("failed", ""),
        "still_running":        stats.get("still_running", ""),
        "avg_duration_sec":     stats.get("avg_sec", ""),
        "p50_duration_sec":     stats.get("p50_sec", ""),
        "p90_duration_sec":     stats.get("p90_sec", ""),
        "min_duration_sec":     stats.get("min_sec", ""),
        "max_duration_sec":     stats.get("max_sec", ""),
        "oc_context":           run_config.get("oc_context", ""),
        "ns_pattern":           run_config.get("ns_pattern", ""),
        "app_label":            run_config.get("app_label", ""),
        "result_dir":           run_dir.name,   # relative path only - portable across machines
        "run_status":           run_status,
    }
    write_header = not registry_file.exists()
    with open(registry_file, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=REGISTRY_FIELDS)
        if write_header:
            w.writeheader()
        w.writerow(row)
    log(f"Registry updated ? {registry_file}")
    return registry_file


# ?? Step 6: Git commit + push ??????????????????????????????????????????????????

def git_commit_and_push(run_config: dict, stats: dict, run_dir: Path,
                        registry_file: Path, do_push: bool, cfg: dict):
    """
    Commit results with proper trailers.
    Only commits:
      - results/<run_id>/*.csv  and  results/<run_id>/run_*.txt/json
      - results/registry_<user_id>.csv
    Does NOT commit pipelineruns_raw.json (excluded via .gitignore).
    """
    repo_root = Path(__file__).parent
    user_id   = run_config["user_id"]
    batch     = run_config["batch_size"]
    date_utc  = run_config["date_utc"]
    status    = stats.get("succeeded", "?")
    failed    = stats.get("failed", "?")
    run_id    = run_config["run_id"]

    commit_msg = (
        f"add: pipeline test results - {user_id} {batch}builds {date_utc}\n"
        f"\n"
        f"Run ID: {run_id}\n"
        f"Batch: {batch} pipeline(s)\n"
        f"Succeeded: {status}  Failed: {failed}\n"
        f"Cluster: {run_config.get('oc_context','')}\n"
        f"\n"
        f"Signed-off-by: {cfg.get('git_name', user_id)} <{cfg.get('git_email', '')}>\n"
        f"Assisted-by: CursorAI"
    )

    # Stage only the relevant files (not raw JSON - .gitignore handles it)
    files_to_add = [
        str(registry_file.relative_to(repo_root)),
        str((run_dir / "run_config.json").relative_to(repo_root)),
        str((run_dir / "run_summary.txt").relative_to(repo_root)),
        str((run_dir / "pipeline_summary.csv").relative_to(repo_root)),
        str((run_dir / "taskrun_detail.csv").relative_to(repo_root)),
    ]
    # Filter to files that actually exist
    files_to_add = [f for f in files_to_add if (repo_root / f).exists()]

    log(f"Staging {len(files_to_add)} file(s) for git commit...")
    result = subprocess.run(["git", "add"] + files_to_add, cwd=repo_root, capture_output=True, text=True)
    if result.returncode != 0:
        log(f"git add failed: {result.stderr.strip()}", "WARN")
        return

    # Check gitlint if available
    lint_result = subprocess.run(
        ["gitlint", "--msg-filename", "-"],
        input=commit_msg, cwd=repo_root, capture_output=True, text=True
    )
    if lint_result.returncode != 0:
        log(f"gitlint warnings:\n{lint_result.stdout.strip()}", "WARN")

    result = subprocess.run(
        ["git", "commit", "-m", commit_msg],
        cwd=repo_root, capture_output=True, text=True
    )
    if result.returncode != 0:
        log(f"git commit failed: {result.stderr.strip()}", "ERROR")
        return
    log(f"git commit successful: {result.stdout.strip().splitlines()[0]}")

    if do_push:
        log("Pushing to remote...")
        push = subprocess.run(["git", "push"], cwd=repo_root, capture_output=True, text=True)
        if push.returncode != 0:
            log(f"git push failed: {push.stderr.strip()}", "ERROR")
        else:
            log("git push successful - colleagues can now `git pull` to see your results")


# ?? Main ???????????????????????????????????????????????????????????????????????

def main():
    args = parse_args()
    cfg  = resolve_config(args)

    batch_size = args.ends_to - args.starts_from + 1
    if batch_size <= 0:
        log("--ends-to must be >= --starts-from", "ERROR")
        sys.exit(1)

    now_utc  = utcnow()
    user_id  = cfg["user_id"]
    run_id   = f"{user_id}_{batch_size}builds_{now_utc.strftime('%Y%m%d_%H%M%S')}"
    run_dir  = RESULTS_DIR / run_id

    run_config = {
        "run_id":        run_id,
        "schema_version": SCHEMA_VERSION,
        "user_id":       user_id,
        "batch_size":    batch_size,
        "starts_from":   args.starts_from,
        "ends_to":       args.ends_to,
        "test_scenario": args.test_scenario,
        "date_utc":      now_utc.strftime("%Y-%m-%d"),
        "start_utc":     now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "local_tz":      local_tz_name(),
        "oc_context":    cfg["oc_context"],
        "ns_pattern":    cfg["ns_pattern"],
        "app_label":     cfg["app_label"],
    }

    do_git_commit = args.git_commit or args.git_push
    do_git_push   = args.git_push

    log(f"Run ID:             {run_id}")
    log(f"Expected pipelines: {batch_size}")
    log(f"oc context:         {cfg['oc_context']}")
    log(f"Local timezone:     {run_config['local_tz']} (stored as UTC)")
    log(f"Results dir:        {run_dir}")
    log(f"Registry:           {RESULTS_DIR / f'registry_{user_id}.csv'}")
    log(f"Git commit/push:    {do_git_commit} / {do_git_push}")

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "run_config.json").write_text(json.dumps(run_config, indent=2))

    if args.dry_run:
        log("DRY RUN - nothing triggered or extracted.")
        return

    # 1. Validate cluster
    if not validate_cluster(cfg["oc_context"]):
        log("Aborting - cluster not reachable.", "ERROR")
        sys.exit(1)

    trigger_time = utcnow()

    # 2. Trigger
    if not args.skip_trigger:
        t0 = time.time()
        trigger_time = trigger_builds(args.starts_from, args.ends_to, args.test_scenario)
        run_config["trigger_duration_sec"] = int(time.time() - t0)
    else:
        # Look back 2 hours so any recently-completed or still-running runs
        # triggered before this invocation are visible to the watcher.
        trigger_time = utcnow() - datetime.timedelta(hours=2)
        log(f"--skip-trigger: watching for pipelines triggered since "
            f"{trigger_time.strftime('%Y-%m-%dT%H:%M:%SZ')} UTC")

    # 3. Watch
    runs = watch_pipeline_runs(
        context      = cfg["oc_context"],
        ns_pattern   = cfg["ns_pattern"],
        app_label    = cfg["app_label"],
        trigger_time = trigger_time,
        expected     = batch_size,
        poll_sec     = cfg["poll_interval"],
        timeout_sec  = cfg["timeout"],
    )
    run_config["end_utc"] = utcnow_str()
    terminal   = sum(1 for r in runs if classify_run(r) in TERMINAL_STATES)
    run_status = "COMPLETE" if terminal >= batch_size else "PARTIAL"

    # 4. Extract
    log("Extracting results (before cluster GC)...")
    stats = extract_results(runs, run_dir, run_config)
    (run_dir / "run_config.json").write_text(json.dumps(run_config, indent=2))

    # 5. Registry
    registry_file = append_registry(run_config, stats, run_dir, run_status)

    # 6. Git
    if do_git_commit:
        git_commit_and_push(run_config, stats, run_dir, registry_file, do_git_push, cfg)
    else:
        log("Tip: re-run with --git-push to commit and share results automatically.")

    log(f"\nDone. run_id={run_id}  status={run_status}")
    log(f"Share results: git add results/ && git commit && git push")
    log(f"Compare all:   python3 compare_runs.py --all")


if __name__ == "__main__":
    main()
