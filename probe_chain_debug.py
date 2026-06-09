"""
probe_chain_debug.py — Trace the full find_spark_job_id() chain step by step.
Run: python3 probe_chain_debug.py
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
print("STEP 1: get_job_history")
print("=" * 60)
jobs = client.get_job_history(PROJECT_ID)
print(f"Total jobs: {len(jobs)}")

failed = [j for j in jobs if j.get('scheduleStatus') == 'FAILED']
print(f"Failed: {len(failed)}")

# Show all unique statuses
statuses = set(j.get('scheduleStatus') for j in jobs)
print(f"All statuses seen: {statuses}")

if not failed:
    print("❌ No FAILED jobs — check status field names above")
    print("First 3 items keys:", [list(j.keys())[:10] for j in jobs[:3]])
    sys.exit(1)

latest = failed[0]
print(f"\nLatest failed item keys: {list(latest.keys())}")
print(f"scheduleMetaId: {latest.get('scheduleMetaId')}")
print(f"workflowMetaId: {latest.get('workflowMetaId')}")
print(f"workflowDagId (direct): {latest.get('workflowDagId')}")

print("\n" + "=" * 60)
print("STEP 2: get_schedule_detail")
print("=" * 60)
schedule_meta_id = latest.get('scheduleMetaId')
if not schedule_meta_id:
    print("❌ No scheduleMetaId in job item")
    sys.exit(1)

sched = client.get_schedule_detail(int(schedule_meta_id))
print(f"Schedule detail keys: {list(sched.keys())}")
print(f"workflowDagId: {sched.get('workflowDagId')}")
print(f"deploymentType: {sched.get('deploymentType')}")
print(f"notebookId: {sched.get('notebookId')}")

workflow_dag_id = sched.get('workflowDagId')
if not workflow_dag_id:
    print("❌ No workflowDagId in schedule detail")
    sys.exit(1)

print("\n" + "=" * 60)
print("STEP 3: find_spark_job_id")
print("=" * 60)
batch_id = sched.get('notebookId')
b_id = int(batch_id) if batch_id else None
print(f"batch_id={b_id}, workflow_dag_id={workflow_dag_id}")

spark_job_id = client.find_spark_job_id(b_id, schedule_meta_id, workflow_dag_id)
print(f"\n✅ spark_job_id = {spark_job_id}" if spark_job_id else "\n❌ spark_job_id = None")

if spark_job_id:
    print("\n" + "=" * 60)
    print("STEP 4: get_rendered_notebook_html")
    print("=" * 60)
    try:
        html = client.get_rendered_notebook_html(spark_job_id, b_id)
        print(f"✅ HTML length: {len(html)} chars")
        print(f"Contains error: {'AirflowException' in html or 'Error' in html}")
    except Exception as e:
        print(f"❌ {e}")
