#!/usr/bin/env python3
"""
Konflux Pipeline Run Comparison Tool
=======================================
Reads ALL results/registry_*.csv files (one per contributor, git-tracked) and
produces:
  1. A terminal comparison table
  2. A self-contained Plotly HTML report ? results/reports/comparison_<ts>.html
     (git-push this file to share with engineering - no server needed)

Git-based collaboration model
------------------------------
Each contributor owns results/registry_<user_id>.csv and commits only that file.
No merge conflicts. Comparison is as simple as:

  git pull                               # gets all colleagues' registries
  python3 compare_runs.py --all          # reads every registry_*.csv automatically
  git add results/reports/ && git push   # share the HTML report

USAGE
-----
  # Compare everything across all contributors:
  python3 compare_runs.py --all

  # Filter by batch size:
  python3 compare_runs.py --batch-size 10

  # Filter by contributor:
  python3 compare_runs.py --user smodak
  python3 compare_runs.py --user smodak --user colleague_id

  # Filter by specific run IDs:
  python3 compare_runs.py --runs smodak_10builds_20260511_134058 colleague_20builds_20260512_093015

  # Skip terminal table, only write HTML:
  python3 compare_runs.py --all --html-only
"""

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

RESULTS_DIR  = Path(__file__).parent / "results"
REPORTS_DIR  = RESULTS_DIR / "reports"

# All possible registry columns - missing ones filled with "" on load
ALL_FIELDS = [
    "run_id", "schema_version", "user_id",
    "batch_size", "starts_from", "ends_to", "test_scenario",
    "date_utc", "start_utc", "end_utc", "trigger_duration_sec",
    "total_pipelines", "succeeded", "failed", "still_running",
    "avg_duration_sec", "p50_duration_sec", "p90_duration_sec",
    "min_duration_sec", "max_duration_sec",
    "oc_context", "ns_pattern", "app_label",
    "result_dir", "run_status",
    # legacy v1.0 names kept for back-compat
    "date", "status",
]

PLOTLY_CDN = "https://cdn.plot.ly/plotly-2.35.2.min.js"


# ?? CLI ????????????????????????????????????????????????????????????????????????

def parse_args():
    p = argparse.ArgumentParser(
        description="Compare Konflux pipeline runs across all contributors",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--all",        action="store_true",  help="Include all runs from all registry_*.csv files")
    group.add_argument("--batch-size", type=int,             help="Filter by batch size")
    group.add_argument("--runs",       nargs="+",            help="Specific run IDs")

    p.add_argument("--user",      action="append", default=None, metavar="USER_ID",
                   help="Filter by user_id (can repeat: --user smodak --user colleague)")
    p.add_argument("--html-only", action="store_true", help="Skip terminal table, write HTML only")
    p.add_argument("--no-html",   action="store_true", help="Skip HTML generation, terminal only")
    return p.parse_args()


# ?? Registry loading ???????????????????????????????????????????????????????????

def parse_utc(ts: str) -> datetime | None:
    if not ts:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%d"):
        try:
            return datetime.strptime(ts, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def normalise_row(row: dict, source_user: str) -> dict:
    """
    Fill missing columns (schema evolution) and fix legacy field names.
    result_dir is stored as a relative path in the registry - resolve it
    against RESULTS_DIR so pipeline_summary.csv can always be found.
    """
    out = {f: row.get(f, "") for f in ALL_FIELDS}
    # Legacy field name migration (schema v1.0 ? v1.1)
    if not out["date_utc"]  and out.get("date"):   out["date_utc"]  = out["date"]
    if not out["run_status"] and out.get("status"): out["run_status"] = out["status"]
    # Infer user_id from the registry filename if absent
    if not out["user_id"]:
        out["user_id"] = source_user
    # Resolve result_dir
    rel = out["result_dir"]
    if rel:
        candidate = RESULTS_DIR / rel
        if candidate.exists():
            out["_result_path"] = str(candidate)
        else:
            out["_result_path"] = rel   # absolute path from old schema
    # Sort key - UTC datetime or raw string
    out["_sort_key"] = parse_utc(out["start_utc"]) or out["start_utc"]
    return out


def load_all_registries() -> list[dict]:
    """
    Glob results/registry_*.csv - one file per contributor.
    Merge into a single list sorted by start_utc (UTC - timezone-safe).
    """
    registry_files = sorted(RESULTS_DIR.glob("registry_*.csv"))
    if not registry_files:
        print(f"ERROR: No registry_*.csv files found in {RESULTS_DIR}", file=sys.stderr)
        print("Run pipeline_orchestrator.py at least once first.", file=sys.stderr)
        sys.exit(1)

    all_rows = []
    seen_ids = set()
    for path in registry_files:
        user_id = path.stem.replace("registry_", "")
        with open(path, newline="") as f:
            for row in csv.DictReader(f):
                norm = normalise_row(row, user_id)
                if norm["run_id"] not in seen_ids:
                    seen_ids.add(norm["run_id"])
                    all_rows.append(norm)

    all_rows.sort(key=lambda r: r["_sort_key"])
    contributors = sorted({r["user_id"] for r in all_rows})
    print(f"Loaded {len(all_rows)} run(s) from {len(registry_files)} registry file(s): "
          f"{', '.join(contributors)}")
    return all_rows


def select_runs(rows: list[dict], args) -> list[dict]:
    result = rows

    if args.batch_size is not None:
        result = [r for r in result if str(r.get("batch_size", "")) == str(args.batch_size)]
        if not result:
            print(f"No runs with batch_size={args.batch_size}", file=sys.stderr)
            sys.exit(1)

    if args.runs:
        id_set = set(args.runs)
        result = [r for r in result if r["run_id"] in id_set]
        missing = id_set - {r["run_id"] for r in result}
        if missing:
            print(f"WARNING: not found in any registry: {missing}", file=sys.stderr)

    if args.user:
        result = [r for r in result if r.get("user_id", "") in args.user]
        if not result:
            print(f"No runs for user(s): {args.user}", file=sys.stderr)
            sys.exit(1)

    return result


# ?? Helpers ????????????????????????????????????????????????????????????????????

def safe_int(v, default=0):
    try:
        return int(v)
    except (ValueError, TypeError):
        return default


def duration_str(seconds) -> str:
    try:
        m, s = divmod(int(seconds), 60)
        return f"{m}m{s:02d}s"
    except (ValueError, TypeError):
        return "N/A"


def load_pipeline_detail(run: dict) -> list[dict]:
    """Load pipeline_summary.csv for a run if available."""
    result_path = run.get("_result_path", "")
    if not result_path:
        return []
    csv_path = Path(result_path) / "pipeline_summary.csv"
    if not csv_path.exists():
        return []
    with open(csv_path, newline="") as f:
        return list(csv.DictReader(f))


# ?? Terminal table ?????????????????????????????????????????????????????????????

def print_table(runs: list[dict]):
    COLS = [
        ("run_id",      28, "<"), ("user",  8, "<"), ("batch", 6, ">"),
        ("date (UTC)", 11, "<"), ("succ",  6, ">"), ("fail",  5, ">"),
        ("avg",        10, ">"), ("p50",  10, ">"), ("p90",  10, ">"),
        ("status",     10, "<"),
    ]
    hdr_str = " ".join(f"{h:{a}{w}}" for h, w, a in COLS)
    sep = "-" * len(hdr_str)
    title = "Konflux Pipeline Run Comparison  (all times UTC)"
    print(f"\n{title:^{len(sep)}}")
    print(sep)
    print(hdr_str)
    print(sep)
    for r in runs:
        print(" ".join([
            f"{r['run_id']:<28}",
            f"{r.get('user_id',''):<8}",
            f"{r.get('batch_size',''):>6}",
            f"{r.get('date_utc',''):>11}",
            f"{r.get('succeeded',''):>6}",
            f"{r.get('failed',''):>5}",
            f"{duration_str(r.get('avg_duration_sec','')):>10}",
            f"{duration_str(r.get('p50_duration_sec','')):>10}",
            f"{duration_str(r.get('p90_duration_sec','')):>10}",
            f"{r.get('run_status',''):>10}",
        ]))
    print(sep + "\n")


# ?? HTML / Plotly report ???????????????????????????????????????????????????????

def build_html(runs: list[dict]) -> str:
    labels    = [r["run_id"] for r in runs]
    users     = [r.get("user_id", "") for r in runs]
    batches   = [safe_int(r.get("batch_size", 0)) for r in runs]
    succeeded = [safe_int(r.get("succeeded", 0)) for r in runs]
    failed    = [safe_int(r.get("failed", 0)) for r in runs]
    avg_secs  = [safe_int(r["avg_duration_sec"]) if r.get("avg_duration_sec") else None for r in runs]
    p50_secs  = [safe_int(r["p50_duration_sec"]) if r.get("p50_duration_sec") else None for r in runs]
    p90_secs  = [safe_int(r["p90_duration_sec"]) if r.get("p90_duration_sec") else None for r in runs]
    min_secs  = [safe_int(r["min_duration_sec"]) if r.get("min_duration_sec") else None for r in runs]
    max_secs  = [safe_int(r["max_duration_sec"]) if r.get("max_duration_sec") else None for r in runs]
    start_utcs = [r.get("start_utc", "") for r in runs]
    statuses  = [r.get("run_status", "") for r in runs]

    all_users    = sorted(set(users))
    success_rates = [round(s / max(s + f, 1) * 100, 1) for s, f in zip(succeeded, failed)]

    # Load per-pipeline rows for the detail table
    per_pipeline = []
    for r in runs:
        rows = load_pipeline_detail(r)
        for row in rows:
            per_pipeline.append({
                "run_id":    r["run_id"],
                "user_id":   r.get("user_id", ""),
                "namespace": row.get("namespace", ""),
                "pipeline":  row.get("pipeline_run", ""),
                "status":    row.get("status", ""),
                "dur_hms":   row.get("duration_hms", ""),
                "failure":   row.get("failure_msg", "")[:120],
            })

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    chart_data     = json.dumps({
        "labels": labels, "users": users, "batches": batches,
        "succeeded": succeeded, "failed": failed,
        "avg": avg_secs, "p50": p50_secs, "p90": p90_secs,
        "min": min_secs, "max": max_secs,
        "rates": success_rates, "start_utcs": start_utcs,
    })
    registry_json  = json.dumps([
        {
            "run_id":    r["run_id"],   "user_id":   r.get("user_id",""),
            "batch":     r.get("batch_size",""), "date_utc": r.get("date_utc",""),
            "start_utc": r.get("start_utc",""),
            "succ":      r.get("succeeded",""),  "fail":  r.get("failed",""),
            "avg":       r.get("avg_duration_sec",""), "p50": r.get("p50_duration_sec",""),
            "p90":       r.get("p90_duration_sec",""),
            "status":    r.get("run_status",""),
            "schema":    r.get("schema_version","1.0"),
        } for r in runs
    ])
    pipeline_json  = json.dumps(per_pipeline)
    contributors   = json.dumps(all_users)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Konflux Pipeline Performance Report</title>
<script src="{PLOTLY_CDN}"></script>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
     background:#0f1117;color:#e2e8f0;padding:24px 32px}}
h1{{font-size:22px;font-weight:700;margin-bottom:4px}}
h2{{font-size:11px;font-weight:700;color:#94a3b8;text-transform:uppercase;
   letter-spacing:.07em;margin-bottom:14px}}
.sub{{color:#64748b;font-size:13px;margin-bottom:24px}}
.notice{{background:#1e2d40;border:1px solid #2d4a6b;border-radius:6px;
         padding:10px 16px;font-size:12px;color:#7dd3fc;margin-bottom:24px}}
.stats-row{{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));
            gap:14px;margin-bottom:28px}}
.stat{{background:#1e2433;border:1px solid #2d3748;border-radius:8px;padding:16px 18px}}
.stat-val{{font-size:26px;font-weight:700;line-height:1;margin-bottom:4px}}
.stat-lbl{{font-size:11px;color:#64748b}}
.green{{color:#22c55e}}.red{{color:#ef4444}}.blue{{color:#3b82f6}}.amber{{color:#f59e0b}}
.chips{{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:22px}}
.chip{{padding:3px 10px;border-radius:12px;font-size:11px;font-weight:600;
      background:#1e3a5f;color:#93c5fd}}
.grid2{{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:24px}}
.grid1{{margin-bottom:24px}}
.card{{background:#1e2433;border:1px solid #2d3748;border-radius:8px;padding:18px 20px}}
.card.full{{grid-column:1/-1}}
table{{width:100%;border-collapse:collapse;font-size:12px}}
thead tr{{background:#151b28}}
th{{padding:8px 12px;text-align:left;font-weight:600;color:#94a3b8;font-size:10px;
   text-transform:uppercase;letter-spacing:.05em}}
td{{padding:7px 12px;border-bottom:1px solid #1a2030;vertical-align:top}}
tr:hover td{{background:#1a2030}}
.badge{{display:inline-block;padding:2px 7px;border-radius:10px;font-size:10px;font-weight:700}}
.ok{{background:#14532d;color:#86efac}}
.fail{{background:#450a0a;color:#fca5a5}}
.partial{{background:#431407;color:#fdba74}}
.tag{{display:inline-block;padding:1px 6px;border-radius:8px;font-size:10px;
     background:#1e3a5f;color:#93c5fd}}
.footer{{color:#334155;font-size:11px;margin-top:24px;border-top:1px solid #1e2433;padding-top:14px}}
input[type=text]{{background:#151b28;border:1px solid #2d3748;border-radius:5px;
                  color:#e2e8f0;padding:6px 10px;font-size:12px;width:260px}}
@media(max-width:680px){{.grid2{{grid-template-columns:1fr}}.card.full{{grid-column:1}}}}
</style>
</head>
<body>

<h1>Konflux Pipeline Performance Report</h1>
<p class="sub">
  stone-stg-rh01 &nbsp; &nbsp; example-packages &nbsp; &nbsp; {now}
  &nbsp; &nbsp; All timestamps in <strong>UTC</strong>
</p>

<div class="notice" id="notice"></div>

<div class="chips" id="chips"></div>
<div class="stats-row" id="stats"></div>

<div class="grid2">
  <div class="card"><h2>Pass / Fail per Run</h2><div id="c-pf"></div></div>
  <div class="card"><h2>Success Rate (%)</h2><div id="c-rate"></div></div>
  <div class="card full"><h2>Duration Breakdown - Min / P50 / Avg / P90 / Max (seconds)</h2><div id="c-dur"></div></div>
  <div class="card full"><h2>Avg Duration Trend Over Time (UTC) - by Contributor</h2><div id="c-trend"></div></div>
</div>

<div class="card grid1">
  <h2>All Runs - Registry</h2>
  <div style="margin-bottom:10px">
    <input type="text" id="run-filter" placeholder="Filter by run ID, user, date " oninput="filterTable()">
  </div>
  <table>
    <thead><tr>
      <th>Run ID</th><th>User</th><th>Batch</th><th>Date (UTC)</th><th>Start (UTC)</th>
      <th>Succeeded</th><th>Failed</th><th>Avg</th><th>P50</th><th>P90</th>
      <th>Schema</th><th>Status</th>
    </tr></thead>
    <tbody id="run-tbody"></tbody>
  </table>
</div>

<div class="card grid1" id="detail-section" style="display:none">
  <h2>Per-Pipeline Detail</h2>
  <table>
    <thead><tr>
      <th>Run ID</th><th>User</th><th>Namespace</th><th>Pipeline Run</th>
      <th>Status</th><th>Duration</th><th>Failure</th>
    </tr></thead>
    <tbody id="detail-tbody"></tbody>
  </table>
</div>

<div class="footer">
  Data source: OpenShift PipelineRun resources &nbsp; &nbsp;
  Captured by pipeline_orchestrator.py &nbsp; &nbsp; Compare by compare_runs.py &nbsp; &nbsp; {now}
</div>

<script>
const D    = {chart_data};
const ROWS = {registry_json};
const PIPE = {pipeline_json};
const CONTRIB = {contributors};

// ?? Notice ?????????????????????????????????????????????????????????
document.getElementById("notice").innerHTML =
  `Comparing <strong>${{ROWS.length}}</strong> run(s) from
   <strong>${{CONTRIB.length}}</strong> contributor(s): ${{CONTRIB.join(", ")}}.
   Runs ordered by <code>start_utc</code> - local timezones are normalized to UTC automatically.`;

// ?? Contributor chips ???????????????????????????????????????????????
CONTRIB.forEach(u => {{
  document.getElementById("chips").innerHTML += `<span class="chip">${{u}}</span>`;
}});

// ?? Top stats ???????????????????????????????????????????????????????
const totalP = D.succeeded.reduce((a,b)=>a+b,0) + D.failed.reduce((a,b)=>a+b,0);
const totalS = D.succeeded.reduce((a,b)=>a+b,0);
const rate   = totalP ? (totalS/totalP*100).toFixed(1) : "N/A";
const avgs   = D.avg.filter(x=>x!=null);
const gAvg   = avgs.length ? Math.round(avgs.reduce((a,b)=>a+b)/avgs.length) : null;
const fmtS   = s => s==null ? "N/A" : `${{Math.floor(s/60)}}m${{String(s%60).padStart(2,"0")}}s`;

[
  {{label:"Runs Captured",     val:ROWS.length,        cls:""}},
  {{label:"Contributors",      val:CONTRIB.length,     cls:"blue"}},
  {{label:"Total Pipelines",   val:totalP,             cls:""}},
  {{label:"Overall Success",   val:rate+"%",           cls:rate>=80?"green":"amber"}},
  {{label:"Grand-Avg Duration",val:fmtS(gAvg),         cls:""}},
].forEach(s => {{
  document.getElementById("stats").innerHTML +=
    `<div class="stat"><div class="stat-val ${{s.cls}}">${{s.val}}</div>
     <div class="stat-lbl">${{s.label}}</div></div>`;
}});

// ?? Plotly shared layout ????????????????????????????????????????????
const L = {{
  paper_bgcolor:"transparent",plot_bgcolor:"transparent",
  font:{{color:"#e2e8f0",size:11}},
  margin:{{t:8,b:70,l:60,r:16}},
  xaxis:{{gridcolor:"#2d3748",tickfont:{{size:9}},tickangle:-30}},
  yaxis:{{gridcolor:"#2d3748"}},
  legend:{{bgcolor:"transparent",font:{{size:10}}}},
}};
const CFG = {{responsive:true}};

// Pass/Fail
Plotly.newPlot("c-pf",[
  {{name:"Succeeded",x:D.labels,y:D.succeeded,type:"bar",marker:{{color:"#22c55e"}}}},
  {{name:"Failed",   x:D.labels,y:D.failed,   type:"bar",marker:{{color:"#ef4444"}}}},
],{{...L,barmode:"stack",height:240}},CFG);

// Success rate
Plotly.newPlot("c-rate",[
  {{x:D.labels,y:D.rates,type:"scatter",mode:"lines+markers",
    line:{{color:"#3b82f6",width:2}},marker:{{color:"#3b82f6",size:7}},name:"Success %"}},
],{{...L,height:240,yaxis:{{...L.yaxis,range:[0,100],ticksuffix:"%"}}}},CFG);

// Duration
Plotly.newPlot("c-dur",[
  {{name:"Min",x:D.labels,y:D.min,type:"bar",marker:{{color:"#334155"}}}},
  {{name:"P50",x:D.labels,y:D.p50,type:"bar",marker:{{color:"#3b82f6"}}}},
  {{name:"Avg",x:D.labels,y:D.avg,type:"bar",marker:{{color:"#6366f1"}}}},
  {{name:"P90",x:D.labels,y:D.p90,type:"bar",marker:{{color:"#f59e0b"}}}},
  {{name:"Max",x:D.labels,y:D.max,type:"bar",marker:{{color:"#ef4444"}}}},
],{{...L,barmode:"group",height:280,yaxis:{{...L.yaxis,ticksuffix:"s"}}}},CFG);

// Trend per contributor
const colors = ["#3b82f6","#22c55e","#f59e0b","#a855f7","#ef4444","#06b6d4"];
Plotly.newPlot("c-trend",
  CONTRIB.map((u,i) => {{
    const pts = ROWS.filter(r => r.user_id===u && r.avg);
    return {{
      name:u, type:"scatter", mode:"lines+markers",
      x:pts.map(r=>r.start_utc), y:pts.map(r=>parseInt(r.avg)),
      line:{{color:colors[i%colors.length],width:2}}, marker:{{size:7}},
    }};
  }}),
  {{...L,height:240,yaxis:{{...L.yaxis,ticksuffix:"s"}},
    xaxis:{{...L.xaxis,title:"Run start time (UTC)"}}}}, CFG);

// ?? Registry table ??????????????????????????????????????????????????
const tbody = document.getElementById("run-tbody");
ROWS.forEach(r => {{
  const badge = r.status==="COMPLETE"?"badge ok":r.status==="PARTIAL"?"badge partial":"badge fail";
  tbody.innerHTML += `<tr>
    <td style="font-family:monospace;font-size:10px">${{r.run_id}}</td>
    <td><span class="tag">${{r.user_id||"?"}}</span></td>
    <td>${{r.batch}}</td><td>${{r.date_utc}}</td>
    <td style="font-size:10px">${{r.start_utc}}</td>
    <td class="green">${{r.succ}}</td>
    <td class="${{r.fail>0?"red":""}}">${{r.fail}}</td>
    <td>${{fmtS(r.avg?parseInt(r.avg):null)}}</td>
    <td>${{fmtS(r.p50?parseInt(r.p50):null)}}</td>
    <td>${{fmtS(r.p90?parseInt(r.p90):null)}}</td>
    <td style="font-size:9px;color:#64748b">${{r.schema||"1.0"}}</td>
    <td><span class="${{badge}}">${{r.status}}</span></td>
  </tr>`;
}});

// ?? Per-pipeline detail table ???????????????????????????????????????
if (PIPE.length > 0) {{
  document.getElementById("detail-section").style.display = "";
  const dtbody = document.getElementById("detail-tbody");
  PIPE.forEach(p => {{
    const tone = p.status==="Succeeded"?"green":p.status==="Failed"?"red":"";
    dtbody.innerHTML += `<tr>
      <td style="font-family:monospace;font-size:10px">${{p.run_id}}</td>
      <td><span class="tag">${{p.user_id}}</span></td>
      <td style="font-size:10px">${{p.namespace}}</td>
      <td style="font-family:monospace;font-size:10px">${{p.pipeline}}</td>
      <td class="${{tone}}">${{p.status}}</td>
      <td>${{p.dur_hms||"-"}}</td>
      <td style="font-size:10px;color:#94a3b8">${{p.failure||""}}</td>
    </tr>`;
  }});
}}

// ?? Live filter ?????????????????????????????????????????????????????
function filterTable() {{
  const q = document.getElementById("run-filter").value.toLowerCase();
  document.querySelectorAll("#run-tbody tr").forEach(tr => {{
    tr.style.display = tr.textContent.toLowerCase().includes(q) ? "" : "none";
  }});
}}
</script>
</body>
</html>"""
    return html


# ?? Main ???????????????????????????????????????????????????????????????????????

def main():
    args = parse_args()
    all_rows = load_all_registries()
    runs     = select_runs(all_rows, args)

    if not runs:
        print("No runs matched.", file=sys.stderr)
        sys.exit(1)

    contributors = sorted({r.get("user_id", "?") for r in runs})
    print(f"\nSelected {len(runs)} run(s) from {len(contributors)} contributor(s): "
          f"{', '.join(contributors)}")
    if runs:
        print(f"Time range (UTC): {runs[0].get('start_utc','?')} ? {runs[-1].get('start_utc','?')}")

    if not args.html_only:
        print_table(runs)

    if not args.no_html:
        html     = build_html(runs)
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        ts       = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        out_path = REPORTS_DIR / f"comparison_{ts}.html"
        out_path.write_text(html, encoding="utf-8")
        print(f"\nHTML report ? {out_path}")
        print("Open in any browser. Share by git-pushing results/reports/.\n")
        print("  git add results/reports/ && git commit -m 'add: comparison report' && git push")


if __name__ == "__main__":
    main()
