# Konflux Pipeline Performance Testing

End-to-end automation for triggering, tracking, extracting, and comparing Konflux pipeline build runs.
Results are stored as CSV files inside this repo so the whole team can `git pull` and compare without
any manual file sharing.

---

## Quick Start

### 1. One-time setup (per user)

Copy the example config and fill in your details:

```bash
cp pipeline_test_config.json.example pipeline_test_config.json
# Edit pipeline_test_config.json -- it is gitignored, so it stays local
```

Minimum config required:

```json
{
  "user_id":    "smodak",
  "oc_context": "default/api-stone-stg-rh01-l2vh-p1-openshiftapps-com:6443/smodak",
  "git_name":   "Subrata Modak",
  "git_email":  "smodak@redhat.com"
}
```

Log in to the cluster first:

```bash
oclogin stone-stg-rh01
```

### 2. Run a test batch

```bash
# Trigger 10 builds, watch them, extract results, and push to git automatically:
python3 pipeline_orchestrator.py \
    --starts-from 1 --ends-to 10 \
    --test-scenario rok \
    --git-push
```

That one command does everything:
- Verifies `oc` access before starting
- Runs `ansible-playbook` to trigger the builds
- Polls every 15 seconds until all 10 pipeline runs reach terminal state
- Extracts CSVs and a human-readable summary immediately (before the cluster GCs old runs)
- GC-resilient: a persistent `seen_runs` cache ensures once a run is observed it is never lost,
  even if the cluster GC-prunes it between polls (Konflux `max-keep-runs` annotation)
- Commits and pushes `results/registry_smodak.csv` and the run's CSVs to git

### 3. Compare results across all contributors

```bash
git pull                       # pick up colleagues' registry files
python3 compare_runs.py --all  # generates terminal table + HTML report
```

### 4. Share the report with engineering

```bash
git add results/reports/
git commit -m "add: pipeline comparison report YYYY-MM-DD"
git push
```
The HTML file is fully self-contained -- engineering can open it in any browser without a server.

---

## Script Reference

### `pipeline_orchestrator.py`

The main driver. Run with `-h` for full flag list.

```
python3 pipeline_orchestrator.py -h
```

**Common invocations:**

```bash
# Standard run (reads config from pipeline_test_config.json):
python3 pipeline_orchestrator.py --starts-from 1 --ends-to 10 --test-scenario rok

# Auto-commit + push after extraction:
python3 pipeline_orchestrator.py --starts-from 1 --ends-to 10 --git-push

# Validate cluster only, do not trigger anything:
python3 pipeline_orchestrator.py --starts-from 1 --ends-to 10 --dry-run

# Already triggered manually, just watch + extract:
python3 pipeline_orchestrator.py --starts-from 1 --ends-to 10 --skip-trigger

# Override any config value on the fly:
python3 pipeline_orchestrator.py \
    --starts-from 1 --ends-to 50 \
    --user-id colleague \
    --oc-context default/api-stone-stg-rh01-l2vh-p1-openshiftapps-com:6443/colleague \
    --poll-interval 30 \
    --timeout 7200
```

**What it writes:**

| Path | Description |
|------|-------------|
| `results/registry_<user_id>.csv` | Master index row for this run (appended) |
| `results/<run_id>/run_config.json` | All parameters + timestamps used |
| `results/<run_id>/pipeline_summary.csv` | One row per pipeline run |
| `results/<run_id>/taskrun_detail.csv` | One row per task run |
| `results/<run_id>/run_summary.txt` | Human-readable summary |
| `results/<run_id>/pipelineruns_raw.json` | Full `oc get pipelinerun` snapshot (gitignored -- large) |

**Run ID format:**

```
<user_id>_<batch_size>builds_<YYYYMMDD>_<HHMMSS>
```

Example: `smodak_10builds_20260511_134058`

All timestamps are UTC regardless of where the script is run. Local timezone is recorded in
`run_config.json` as metadata only and is never used for ordering or comparison.

---

### `compare_runs.py`

Reads all `results/registry_*.csv` files (one per contributor) and produces a comparison.
Run with `-h` for full flag list.

```
python3 compare_runs.py -h
```

**Common invocations:**

```bash
# Compare all runs from all contributors:
python3 compare_runs.py --all

# Only look at 10-build runs:
python3 compare_runs.py --batch-size 10

# Only your runs:
python3 compare_runs.py --user smodak

# Two specific run IDs side by side:
python3 compare_runs.py --runs smodak_10builds_20260511_134058 colleague_20builds_20260512_093015

# Terminal table only (no HTML):
python3 compare_runs.py --all --no-html

# HTML report only (no terminal table):
python3 compare_runs.py --all --html-only
```

**Output:**

- Terminal table comparing all selected runs (columns: run_id, user, batch, date UTC,
  succeeded, failed, avg, p50, p90, status)
- `results/reports/comparison_<timestamp>.html` -- interactive Plotly report

---

### `export_pipeline_metrics.py`

Lower-level utility: reads `oc get pipelinerun -o json` from stdin and writes a CSV to stdout.
Useful for one-off ad-hoc extraction without the full orchestrator flow.

```
python3 export_pipeline_metrics.py -h
```

**Example:**

```bash
oc get pipelinerun --all-namespaces -o json \
  --context default/api-stone-stg-rh01-l2vh-p1-openshiftapps-com:6443/smodak \
  | python3 export_pipeline_metrics.py \
      --ns-filter "test-rhtap-[0-9]+-tenant" \
      --app-filter example-packages \
  > ad_hoc_$(date +%Y%m%d_%H%M%S).csv
```

---

## Architecture

### Data flow

```
ansible-playbook
  (creates GitHub PRs)
        |
        v
  Konflux / PAC                          oc cluster
  (PAC sees PR, triggers PipelineRun)       |
        |                                   |
        +------- pipeline_orchestrator.py --+
                   polls every 15s
                   persistent seen_runs cache
                   (GC-resilient)
                   until all terminal
                        |
                        v
              results/<run_id>/
                pipeline_summary.csv
                taskrun_detail.csv
                run_config.json
                run_summary.txt
                        |
                        v
              results/registry_<user_id>.csv  <-- git tracked
                        |
                        v
                    git push
                        |
                     (team pulls)
                        |
                        v
              compare_runs.py
              (globs all registry_*.csv)
                        |
                        v
              results/reports/comparison_<ts>.html
```

### Git collaboration model (no merge conflicts)

Each contributor owns exactly one file: `results/registry_<user_id>.csv`.
This file is append-only -- new rows are added at the bottom after each run.
Because no two contributors share a registry file, simultaneous `git push` operations
never produce a merge conflict.

```
results/
  registry_smodak.csv        <- smodak's runs only
  registry_colleague.csv     <- colleague's runs only (different timezone, different oc-context)
  smodak_10builds_20260511_134058/
    pipeline_summary.csv
    taskrun_detail.csv
    run_config.json
    run_summary.txt
  colleague_20builds_20260512_093015/
    pipeline_summary.csv
    ...
  reports/
    comparison_20260512_120000.html   <- shared report (git pushed)
```

### Schema versioning

Every CSV row carries a `schema_version` field (currently `1.1`).
When new columns are added in future versions, `compare_runs.py` fills missing columns
with empty strings so old and new datasets always compare without errors.
Bump `SCHEMA_VERSION` in `pipeline_orchestrator.py` whenever the registry schema changes.

### Timezone handling

All timestamps are stored in UTC. The `run_id` contains a UTC timestamp, so runs from
colleagues in JST, IST, or EST are sorted and compared correctly without any conversion.
The machine's local timezone is captured in `run_config.json` as `local_tz` for reference,
but it is never used for ordering.

---

## Generating and Presenting Reports

### HTML report structure

The Plotly HTML report (`results/reports/comparison_<timestamp>.html`) contains:

| Chart | What it shows |
|-------|---------------|
| Pass / Fail per Run | Stacked bar -- succeeded vs failed per run, colored green/red |
| Success Rate (%) | Line chart trending success rate over time |
| Duration Breakdown | Grouped bars: Min / P50 / Avg / P90 / Max per run |
| Avg Duration Trend | Line per contributor, x-axis = UTC start time, y-axis = avg seconds |
| All Runs Registry | Full searchable table with schema version, status badge |
| Per-Pipeline Detail | One row per pipeline with namespace, status, duration, failure message |

### Presenting to engineering

1. Open `results/reports/comparison_<timestamp>.html` in a browser.
2. All charts are interactive -- hover for tooltips, click legend to show/hide series.
3. For slides: use the camera icon in any Plotly chart to export a PNG of that chart.
4. For a live demo: share the HTML file directly (works offline, no server).
5. For a Confluence/Jira attachment: attach the HTML file directly.

### Reading the duration chart

- **Min**: fastest successful pipeline (best case)
- **P50 (median)**: typical pipeline -- half finish faster, half slower
- **Avg**: mean duration -- pulled up by slow outliers
- **P90**: 90th percentile -- 90% of pipelines finish within this time
- **Max**: slowest pipeline (worst case)

A large gap between P90 and Max indicates occasional outliers worth investigating.
A large gap between Avg and P50 indicates skewed distribution (some very slow runs pulling up the mean).

---

## Future Work / Roadmap

| Priority | Item | Description |
|----------|------|-------------|
| High | Parallel pipeline triggering | Currently sequential via `ansible-playbook`. Add `--parallel N` to trigger N at a time |
| High | Failure pattern grouping | Auto-cluster failures by task name and error type in the HTML report |
| Medium | Splunk integration | Pull task-level wait/exec times directly from Splunk API instead of manual PDF export |
| Medium | GitHub Actions trigger | Trigger test runs from a GitHub Actions workflow on a cron schedule |
| Medium | Trend alerting | Detect when P90 duration exceeds a threshold compared to the rolling average |
| Low | Per-architecture breakdown | Break duration and failure rate out by architecture (x86_64, arm64, ppc64le, s390x) |
| Low | Cost estimation | Estimate compute cost per run based on VM type and duration |
| Low | Retry tracking | Track how many pipelines needed a retry before succeeding |
| Low | Dashboard server | Serve `results/reports/` as a static site (GitHub Pages or internal) |

---

## Troubleshooting

### "oc whoami failed" / cluster not reachable

```bash
oclogin stone-stg-rh01
# Then re-run the orchestrator
```

### Pipeline runs not being detected

Check that `--ns-pattern` matches your tenant namespaces and `--app-label` matches the
application name. Use `--dry-run` to validate without triggering anything.

```bash
oc get pipelinerun --all-namespaces \
  --context default/api-stone-stg-rh01-l2vh-p1-openshiftapps-com:6443/smodak \
  | grep example-packages
```

### "No registry_*.csv files found"

No runs have been captured yet. Run `pipeline_orchestrator.py` at least once, or
`git pull` to fetch colleagues' registries.

### Git commit fails / gitlint errors

The orchestrator runs `gitlint` before committing. Ensure `gitlint` is installed:

```bash
pip install gitlint
```

### Partial results (PARTIAL status in registry)

The orchestrator timed out before all pipelines finished. The `--timeout` default is 3600s (1 hour).
Increase it for large batch sizes:

```bash
python3 pipeline_orchestrator.py --starts-from 1 --ends-to 50 --timeout 7200
```

### Some pipeline runs are missing from the extracted results

The cluster GCs pipeline runs aggressively via PAC's `max-keep-runs: 3` annotation.
The orchestrator uses three layers to fight this:

1. **15s poll interval** (default) - faster polling reduces the window in which a
   completed run can be pruned before the orchestrator sees it.
2. **Persistent `seen_runs` cache** - once a run is observed it is stored in memory
   and never dropped, even if the cluster deletes it before the next poll.

If runs are still missing, they were GC'd before the very first poll. Increase
PAC's `max-keep-runs` annotation in your `.tekton/` YAML to give the orchestrator
more time, or lower `poll_interval` in your config file.

### Two contributors' runs appear out of order in the comparison

All runs are sorted by `start_utc`. If a run appears at the wrong position, check that
the machine's system clock is correct (NTP synced). The orchestrator stores all timestamps in UTC
so local timezone setting does not matter, but an incorrect system clock would affect ordering.
