"""
verify_may8_fix.py — End-to-end test for the May 8th date limitation fix.

Run after refreshing probe_creds.txt to verify:
1. get_job_history() now returns May 8th records (via activeOnly=false)
2. find_spark_job_id() finds the sparkJobId (via task logs or XCom fallback)
3. Notebook HTML is retrievable for May 8th runs
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from data_layer import ElementMLClient

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

client = ElementMLClient(BEARER, cookies)
PROJECT_ID = 16701

print("=" * 60)
print("STEP 1: get_job_history() — check total records and date range")
print("=" * 60)
jobs = client.get_job_history(PROJECT_ID)
print(f"Total jobs returned: {len(jobs)}")

if jobs:
    dates = sorted(set(str(j.get('scheduleStartDate', ''))[:10] for j in jobs if j.get('scheduleStartDate')))
    print(f"Date range: {dates[0]} → {dates[-1]}")

    may8 = [j for j in jobs if
            str(j.get('scheduleStartDate', ''))[:10] == '2026-05-08' or
            str(j.get('scheduleEndDate', ''))[:10] == '2026-05-08']
    print(f"\nMay 8th records: {len(may8)}")
    for j in may8:
        print(f"  scheduleMetaId={j.get('scheduleMetaId')} | "
              f"status={j.get('scheduleStatus')} | "
              f"start={str(j.get('scheduleStartDate',''))[:16]} | "
              f"end={str(j.get('scheduleEndDate',''))[:16]}")

print("\n" + "=" * 60)
print("STEP 2: find_spark_job_id() for May 8th failure")
print("=" * 60)

failed_may8 = [j for j in jobs if
               j.get('scheduleStatus') == 'FAILED' and (
                   str(j.get('scheduleStartDate', ''))[:10] == '2026-05-08' or
                   str(j.get('scheduleEndDate', ''))[:10] == '2026-05-08')]

if not failed_may8:
    print("No FAILED runs on May 8th — check dates above")
    # Show all failed runs for debugging
    all_failed = [j for j in jobs if j.get('scheduleStatus') == 'FAILED']
    print(f"\nAll failed runs: {len(all_failed)}")
    for j in all_failed[:5]:
        print(f"  {str(j.get('scheduleEndDate',''))[:10]} scheduleMetaId={j.get('scheduleMetaId')}")
    sys.exit(0)

run = failed_may8[0]
print(f"Using: scheduleMetaId={run.get('scheduleMetaId')} | "
      f"workflowDagId={run.get('workflowDagId')[:30]}...")

sched = client.get_schedule_detail(int(run.get('scheduleMetaId')))
workflow_dag_id = sched.get('workflowDagId')
notebook_id = sched.get('notebookId')
b_id = int(notebook_id) if notebook_id else None

print(f"workflowDagId={workflow_dag_id}")
print(f"notebookId={notebook_id}")

print("\nFinding sparkJobId (may use XCom fallback for old runs)...")
spark_job_id = client.find_spark_job_id(
    b_id,
    run.get('scheduleMetaId'),
    workflow_dag_id,
    run_index=0,
    target_execution_date=run.get('scheduleStartDate'),
)

if spark_job_id:
    print(f"✅ spark_job_id = {spark_job_id}")

    print("\n" + "=" * 60)
    print("STEP 3: Fetch notebook HTML")
    print("=" * 60)
    try:
        html = client.get_rendered_notebook_html(spark_job_id, b_id)
        print(f"✅ HTML length: {len(html)} chars")
        has_error = any(kw in html for kw in ['AirflowException', 'Error', 'Traceback', 'exception'])
        print(f"Contains error markers: {has_error}")
    except Exception as e:
        print(f"❌ {e}")
else:
    print("❌ spark_job_id = None")
    print("\nThis means BOTH task log AND XCom are empty for this run.")
    print("The run may be too old for Airflow to retain ANY metadata.")
    print("Next step: ask the user for the DevTools network URL from the May 8th job view.")

print("\nDone.")
