#!/usr/bin/env python3
"""
Export PipelineRun metrics to CSV from an OpenShift cluster.

Usage:
    oc get pipelinerun --all-namespaces -o json \
      --context=default/api-stone-stg-rh01-l2vh-p1-openshiftapps-com:6443/smodak \
      | python3 export_pipeline_metrics.py > results_10builds_<YYYYMMDD_HHMM>.csv

Filter by namespace pattern (e.g. only test-rhtap-1..10):
    oc get pipelinerun --all-namespaces -o json \
      --context=default/api-stone-stg-rh01-l2vh-p1-openshiftapps-com:6443/smodak \
      | python3 export_pipeline_metrics.py --ns-filter "test-rhtap-[0-9]+-tenant" \
                                           --app-filter "example-packages"
"""
import json
import sys
import csv
import datetime
import re
import argparse


def parse_args():
    parser = argparse.ArgumentParser(description="Export PipelineRun data to CSV")
    parser.add_argument("--ns-filter",  default=None, help="Regex to filter namespaces")
    parser.add_argument("--app-filter", default=None, help="Filter by application label")
    parser.add_argument("--comp-filter", default=None, help="Filter by component label")
    return parser.parse_args()


def duration_str(seconds):
    if seconds == "":
        return ""
    m, s = divmod(int(seconds), 60)
    return f"{m}m{s:02d}s"


def main():
    args = parse_args()
    data = json.load(sys.stdin)
    rows = []

    for pr in data.get("items", []):
        ns   = pr["metadata"]["namespace"]
        name = pr["metadata"]["name"]
        labels = pr["metadata"].get("labels", {})
        app  = labels.get("appstudio.openshift.io/application", "")
        comp = labels.get("appstudio.openshift.io/component", "")

        # Apply filters
        if args.ns_filter  and not re.search(args.ns_filter,  ns):   continue
        if args.app_filter and args.app_filter != app:                 continue
        if args.comp_filter and args.comp_filter != comp:              continue

        conds   = pr.get("status", {}).get("conditions", [])
        status  = conds[-1].get("reason", "Unknown") if conds else "Unknown"
        message = conds[-1].get("message", "")[:200] if conds else ""

        start_str = pr.get("status", {}).get("startTime", "")
        end_str   = pr.get("status", {}).get("completionTime", "")

        duration_sec = ""
        if start_str and end_str:
            fmt = "%Y-%m-%dT%H:%M:%SZ"
            try:
                duration_sec = int(
                    (datetime.datetime.strptime(end_str, fmt) -
                     datetime.datetime.strptime(start_str, fmt)).total_seconds()
                )
            except Exception:
                pass

        rows.append({
            "namespace":      ns,
            "pipeline_run":   name,
            "application":    app,
            "component":      comp,
            "status":         status,
            "start_time_utc": start_str,
            "end_time_utc":   end_str,
            "duration_sec":   duration_sec,
            "duration_hms":   duration_str(duration_sec),
            "failure_msg":    message,
        })

    if not rows:
        print("No matching pipeline runs found.", file=sys.stderr)
        sys.exit(1)

    # Summary to stderr (won't pollute the CSV)
    succeeded = [r for r in rows if r["status"] == "Succeeded"]
    failed    = [r for r in rows if r["status"] == "Failed"]
    running   = [r for r in rows if r["status"] not in ("Succeeded", "Failed")]
    durations = [r["duration_sec"] for r in succeeded if r["duration_sec"] != ""]

    print(f"\n=== Summary ===", file=sys.stderr)
    print(f"Total pipelines : {len(rows)}", file=sys.stderr)
    print(f"  Succeeded     : {len(succeeded)}", file=sys.stderr)
    print(f"  Failed        : {len(failed)}", file=sys.stderr)
    print(f"  Still running : {len(running)}", file=sys.stderr)
    if durations:
        avg = sum(durations) / len(durations)
        durations.sort()
        median = durations[len(durations) // 2]
        p90 = durations[int(len(durations) * 0.90)]
        print(f"  Avg duration  : {duration_str(avg)}", file=sys.stderr)
        print(f"  Median        : {duration_str(median)}", file=sys.stderr)
        print(f"  P90           : {duration_str(p90)}", file=sys.stderr)
        print(f"  Min           : {duration_str(min(durations))}", file=sys.stderr)
        print(f"  Max           : {duration_str(max(durations))}", file=sys.stderr)
    print(f"================\n", file=sys.stderr)

    writer = csv.DictWriter(sys.stdout, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)


if __name__ == "__main__":
    main()
