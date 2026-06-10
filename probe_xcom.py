"""
probe_xcom.py — Try Airflow XCom, renderedFields, and task instance metadata
for an OLD dag run (targeting ~May 8th).

Task logs are purged after ~2-3 weeks; XCom and other DB-backed metadata
survive much longer. If sparkJobId is stored in XCom or rendered fields,
we can retrieve it without task logs.

Chain:
  1. dagUrl → slug → airflow_base (already known working)
  2. GET /dagRuns?execution_date_gte=2026-05-08... → get old dagRunId
  3. GET /dagRuns/{runId}/taskInstances → task list + task_instance fields
  4. GET /dagRuns/{runId}/taskInstances/{taskId}/xcomEntries → check XCom
  5. GET /dagRuns/{runId}/taskInstances/{taskId}/renderedFields → check params
"""
import requests, re, json, os, urllib3
from urllib.parse import quote
urllib3.disable_warnings()

cred_file = os.path.join(os.path.dirname(__file__), "probe_creds.txt")
with open(cred_file) as f:
    lines = f.read().splitlines()
BEARER = lines[0].replace("Bearer ", "").strip()
COOKIES_RAW = lines[1].strip() if len(lines) > 1 else ""
cookies = {}
for part in COOKIES_RAW.split(";"):
    if "=" in part:
        k, v = part.strip().split("=", 1)
        cookies[k.strip()] = v.strip()

headers = {"Authorization": f"Bearer {BEARER}", "Accept": "application/json"}
API_BASE = "https://ml.prod.walmart.com:31200"
WORKFLOW_DAG_ID = "dag_48227_fe957158-dff8-43c0-ae7c-813cb13f5dab"
TARGET_OLD_DATE = "2026-05-08"  # Date we know works in UI but fails in agent

# ── Step 1: Get Airflow base URL ─────────────────────────────────────────────
print("Step 1: Get Airflow slug...")
r = requests.get(
    f"{API_BASE}/v1/workflows/dag/{WORKFLOW_DAG_ID}/dagUrl?type=dagRun",
    headers=headers, cookies=cookies, verify=False, timeout=15
)
print(f"  [{r.status_code}] dagUrl response")
if r.status_code != 200:
    print("  FAILED — exiting")
    exit(1)

airflow_ui_url = r.json().get("url", "")
slug_match = re.search(r":\d+(/[^/]+/airflow)", airflow_ui_url)
if not slug_match:
    print(f"  No slug in URL: {airflow_ui_url}")
    exit(1)

airflow_path = slug_match.group(1)
airflow_base = f"https://ml.prod.walmart.com:31206{airflow_path}/api/v1"
print(f"  airflow_base = {airflow_base}")

# ── Step 2: Get dagRuns for OLD date ─────────────────────────────────────────
print(f"\nStep 2: Get dagRuns for {TARGET_OLD_DATE}...")
r2 = requests.get(
    f"{airflow_base}/dags/{WORKFLOW_DAG_ID}/dagRuns"
    f"?limit=10&order_by=-execution_date"
    f"&execution_date_gte={TARGET_OLD_DATE}T00:00:00+00:00"
    f"&execution_date_lte={TARGET_OLD_DATE}T23:59:59+00:00",
    headers=headers, cookies=cookies, verify=False, timeout=20
)
print(f"  [{r2.status_code}]")
if r2.status_code != 200:
    print(f"  Body: {r2.text[:400]}")
    # Try without date filter — get many recent runs and look for old ones
    print("  Falling back to limit=200 to find old runs...")
    r2 = requests.get(
        f"{airflow_base}/dags/{WORKFLOW_DAG_ID}/dagRuns"
        f"?limit=200&order_by=-execution_date",
        headers=headers, cookies=cookies, verify=False, timeout=30
    )
    print(f"  [{r2.status_code}]")

if r2.status_code != 200:
    print("  Cannot get dagRuns — exiting")
    exit(1)

dag_runs = r2.json().get("dag_runs", [])
print(f"  Total dagRuns returned: {len(dag_runs)}")

# Show date range
if dag_runs:
    dates = [dr.get("execution_date", "")[:10] for dr in dag_runs]
    print(f"  Date range: {min(dates)} → {max(dates)}")

# Find runs matching target date
target_runs = [dr for dr in dag_runs
               if dr.get("execution_date", "").startswith(TARGET_OLD_DATE)]
if not target_runs:
    # Widen to nearest date
    print(f"  No exact match for {TARGET_OLD_DATE}")
    # Show all available dates for debugging
    unique_dates = sorted(set(dr.get("execution_date", "")[:10] for dr in dag_runs))
    print(f"  Available dates: {unique_dates}")
    # Try to pick runs from a month+ ago
    old_runs = [dr for dr in dag_runs
                if dr.get("execution_date", "")[:10] < "2026-05-25"]
    if old_runs:
        print(f"  Using {len(old_runs)} old run(s) (pre-May-25)")
        target_runs = old_runs[:3]
    else:
        print("  No runs older than May 25 available")
        exit(1)
else:
    print(f"  Found {len(target_runs)} runs on {TARGET_OLD_DATE}")

# ── Steps 3–5: For each old dagRun, probe all metadata endpoints ─────────────
for run in target_runs[:3]:
    dag_run_id = run.get("dag_run_id", "")
    exec_date = run.get("execution_date", "")
    state = run.get("state", "")
    conf = run.get("conf", {})
    print(f"\n{'='*70}")
    print(f"dagRun: {dag_run_id}")
    print(f"  execution_date={exec_date} state={state}")
    print(f"  conf={json.dumps(conf)[:300]}")
    print(f"  ALL FIELDS: {list(run.keys())}")
    print(f"  FULL OBJECT: {json.dumps(run)[:600]}")

    dag_run_id_enc = quote(dag_run_id, safe="")

    # Step 3: Get task instances
    print(f"\n  Step 3: taskInstances...")
    ti_r = requests.get(
        f"{airflow_base}/dags/{WORKFLOW_DAG_ID}/dagRuns/{dag_run_id_enc}/taskInstances",
        headers=headers, cookies=cookies, verify=False, timeout=15
    )
    print(f"  [{ti_r.status_code}]")
    if ti_r.status_code != 200:
        print(f"  Body: {ti_r.text[:400]}")
        continue

    tasks = ti_r.json().get("task_instances", [])
    print(f"  {len(tasks)} task(s)")

    for task in tasks:
        task_id = task.get("task_id")
        try_number = task.get("try_number", 1)
        task_state = task.get("state")
        print(f"\n  Task: {task_id} | state={task_state} | try={try_number}")
        print(f"  All fields: {list(task.keys())}")
        # Print full task object — might contain spark_job_id directly
        print(f"  Full: {json.dumps(task)[:800]}")

        task_id_enc = quote(task_id or "", safe="")
        if not task_id:
            continue

        # Step 4a: XCom entries (list)
        print(f"\n    Step 4a: XCom entries...")
        xcom_r = requests.get(
            f"{airflow_base}/dags/{WORKFLOW_DAG_ID}/dagRuns/{dag_run_id_enc}"
            f"/taskInstances/{task_id_enc}/xcomEntries",
            headers=headers, cookies=cookies, verify=False, timeout=15
        )
        print(f"    [{xcom_r.status_code}] Body[:600]: {xcom_r.text[:600]}")

        # Step 4b: XCom return_value specifically
        print(f"\n    Step 4b: XCom return_value...")
        xcom_rv = requests.get(
            f"{airflow_base}/dags/{WORKFLOW_DAG_ID}/dagRuns/{dag_run_id_enc}"
            f"/taskInstances/{task_id_enc}/xcomEntries/return_value",
            headers=headers, cookies=cookies, verify=False, timeout=15
        )
        print(f"    [{xcom_rv.status_code}] Body[:600]: {xcom_rv.text[:600]}")

        # Step 5: Rendered fields
        print(f"\n    Step 5: renderedFields...")
        rf_r = requests.get(
            f"{airflow_base}/dags/{WORKFLOW_DAG_ID}/dagRuns/{dag_run_id_enc}"
            f"/taskInstances/{task_id_enc}/renderedFields",
            headers=headers, cookies=cookies, verify=False, timeout=15
        )
        print(f"    [{rf_r.status_code}] Body[:600]: {rf_r.text[:600]}")

        # Step 6: Task instance details (individual — might have extra fields)
        print(f"\n    Step 6: taskInstance detail...")
        ti_detail = requests.get(
            f"{airflow_base}/dags/{WORKFLOW_DAG_ID}/dagRuns/{dag_run_id_enc}"
            f"/taskInstances/{task_id_enc}",
            headers=headers, cookies=cookies, verify=False, timeout=15
        )
        print(f"    [{ti_detail.status_code}] Body[:600]: {ti_detail.text[:600]}")

        # Step 7: Try the task log anyway (will likely fail but check error)
        print(f"\n    Step 7: task log (expect failure)...")
        log_r = requests.get(
            f"{airflow_base}/dags/{WORKFLOW_DAG_ID}/dagRuns/{dag_run_id_enc}"
            f"/taskInstances/{task_id_enc}/logs/{try_number}",
            headers={**headers, "Accept": "text/plain"},
            cookies=cookies, verify=False, timeout=15
        )
        print(f"    [{log_r.status_code}] Body[:400]: {log_r.text[:400]}")

print("\nDone.")
