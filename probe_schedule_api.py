"""
probe_schedule_api.py — Call the confirmed working API endpoints discovered from JS.

JS confirmed:
1. fetchWorkflowSchedule → GET /v1/workflows/schedules/{scheduleId}
2. fetchAllSchedules → GET /v1/workflows/schedules?projectId=X&activeOnly=false&archived=false

Also search JS for:
- How BATCH type jobs show run history (different from Schedule type)
- How the /v1/jobs/{spark_job_id} path is constructed
"""
import requests, json, os, urllib3
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
MAIN = "https://ml.prod.walmart.com"
WF = f"{MAIN}:31200"

PROJECT_ID = "16701"
WORKFLOW_META_ID = "48227"
SCHEDULE_META_ID = "895958"
BATCH_ID = "88538"

def get(url, label=""):
    try:
        r = requests.get(url, headers=headers, cookies=cookies, verify=False, timeout=20)
        label = label or url
        print(f"\n  [{r.status_code}] {label}")
        if r.status_code == 200:
            print(f"  ✅ SUCCESS!")
            # Pretty print if JSON
            try:
                data = r.json()
                print(json.dumps(data, indent=2)[:4000])
            except:
                print(r.text[:2000])
        else:
            print(f"  Body: {r.text[:600]}")
        return r
    except Exception as e:
        print(f"\n  [ERR] {label}: {e}")
        return None

print("=" * 70)
print("CONFIRMED API: fetchWorkflowSchedule")
print("=" * 70)
# JS: GET /v1/workflows/schedules/{scheduleId}
r = get(f"{WF}/v1/workflows/schedules/{SCHEDULE_META_ID}", f"/v1/workflows/schedules/{SCHEDULE_META_ID}")

print("\n" + "=" * 70)
print("CONFIRMED API: fetchAllSchedules")
print("=" * 70)
# JS: GET /v1/workflows/schedules?params
for qp in [
    f"projectId={PROJECT_ID}&activeOnly=false&archived=false",
    f"projectId={PROJECT_ID}&activeOnly=false&archived=false&workflowId={WORKFLOW_META_ID}",
    f"projectId={PROJECT_ID}",
]:
    get(f"{WF}/v1/workflows/schedules?{qp}", f"/v1/workflows/schedules?{qp}")

print("\n" + "=" * 70)
print("SEARCHING JS: How /v1/jobs/ URL is constructed for notebook HTML")
print("=" * 70)
r = requests.get(f"{MAIN}/element/static/js/main.7c45117e.js",
                 headers=headers, cookies=cookies, verify=False, timeout=60)
js = r.text

# The notebook URL is: /v1/jobs/{spark_job_id}/batch-{batch_id}-{spark_job_id}.html
# Search for "jobs" near "notebooks" base
for term in ['"/jobs/"', "'/jobs/'", "/jobs/", "jobs/", ".html", "batch-"]:
    idx = 0; count = 0
    while count < 5:
        pos = js.find(term, idx)
        if pos == -1: break
        ctx = js[max(0, pos-250):pos+350]
        if any(k in ctx for k in ["31001", "notebooks", "notebookUrl", "concat(", "spark", "batch"]):
            count += 1
            print(f"\n  ✅ '{term}' pos={pos}: ...{ctx}...")
        idx = pos + len(term)
    if count == 0 and js.find(term) != -1:
        print(f"  ℹ️  '{term}' found but not near notebook/31001 context")

# Search for how BATCH type jobs are handled (different from schedule type)
print("\n" + "=" * 70)
print("SEARCHING JS: BATCH type handling (different from schedule)")
print("=" * 70)
for term in ['"BATCH"', "'BATCH'", "isBatch", "is_batch", "type===", 'type=="BATCH"', "batchType"]:
    idx = 0; count = 0
    while count < 3:
        pos = js.find(term, idx)
        if pos == -1: break
        ctx = js[max(0, pos-200):pos+400]
        if any(k in ctx for k in ["runs", "schedule", "workflow", "job", "run"]):
            count += 1
            print(f"\n  [{count}] '{term}' pos={pos}: ...{ctx}...")
        idx = pos + len(term)

# Search for "latestJobId" — from the fetch call we saw it passing latestJobId
print("\n" + "=" * 70)
print("SEARCHING JS: latestJobId")
print("=" * 70)
for term in ["latestJobId", "latest_job_id", "latestJob", "lastJobId"]:
    pos = js.find(term)
    if pos != -1:
        ctx = js[max(0, pos-300):pos+500]
        print(f"\n  ✅ '{term}' pos={pos}: ...{ctx}...")
    else:
        print(f"  ✗ '{term}' not found")

# Search for fetchLatestSchedules implementation
print("\n" + "=" * 70)
print("SEARCHING JS: fetchLatestSchedules IMPLEMENTATION")
print("=" * 70)
idx = 0
while True:
    pos = js.find("fetchLatestSchedules", idx)
    if pos == -1: break
    ctx = js[max(0, pos-50):pos+800]
    if "yield" in ctx or "concat" in ctx or "T1" in ctx:
        print(f"\n  pos={pos}: ...{ctx}...")
        break
    idx = pos + len("fetchLatestSchedules")

# Also look for what fields are in the schedule object
print("\n" + "=" * 70)
print("SEARCHING JS: schedule object fields (latestRun, sparkJobId, etc.)")
print("=" * 70)
for term in ["latestRunId", "latestRun", "sparkJobId", "spark_job_id", "jobId", "executionId",
             "runId", "currentRunId", "activeRunId"]:
    idx = 0; count = 0
    while count < 3:
        pos = js.find(term, idx)
        if pos == -1: break
        ctx = js[max(0, pos-200):pos+300]
        if any(k in ctx for k in ["schedule", "workflow", "run", "batch", "job"]):
            count += 1
            print(f"\n  [{count}] '{term}' pos={pos}: ...{ctx}...")
        idx = pos + len(term)
    if count == 0:
        total = 0
        tp = js.find(term)
        while tp != -1:
            total += 1
            tp = js.find(term, tp + len(term))
        if total > 0:
            print(f"  ℹ️  '{term}' found {total}x but not near schedule/run context")
        else:
            print(f"  ✗ '{term}' not found")

print("\nDone.")
