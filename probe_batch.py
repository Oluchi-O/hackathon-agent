"""
probe_batch.py — Find the API endpoint for BATCH type workflow run history.

Key discovery: scheduleMetaId=895958 is type BATCH (not Schedule).
The /runs endpoint returned: "Runs can only be viewed for Schedules. Job Id 895958 is of type BATCH"
OPTIONS on /v1/workflows/schedules/895958/runs → Allow: PUT,OPTIONS only.

Now we need to find the endpoint for BATCH workflow run history.
"""
import requests, os, urllib3
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

headers_get = {"Authorization": f"Bearer {BEARER}", "Accept": "application/json"}
MAIN = "https://ml.prod.walmart.com"
WF = f"{MAIN}:31200"

PROJECT_ID = "16701"
WORKFLOW_META_ID = "48227"
SCHEDULE_META_ID = "895958"
BATCH_ID = "88538"
DAG_ID = "dag_48227_fe957158-dff8-43c0-ae7c-813cb13f5dab"

def get(url, label=""):
    try:
        r = requests.get(url, headers=headers_get, cookies=cookies, verify=False, timeout=15)
        body = r.text[:2000]
        label = label or url
        print(f"\n  [{r.status_code}] {label}")
        if r.status_code == 200:
            print(f"  ✅ SUCCESS! Body (first 2000 chars): {body}")
        else:
            print(f"  Body: {body[:500]}")
        return r
    except Exception as e:
        print(f"\n  [ERR] {label}: {e}")
        return None

print("=" * 70)
print("BATCH TYPE JOB ENDPOINTS")
print("=" * 70)

# BATCH-specific endpoints
for path in [
    f"/v1/workflows/{WORKFLOW_META_ID}/batch/{SCHEDULE_META_ID}",
    f"/v1/workflows/{WORKFLOW_META_ID}/batch/{SCHEDULE_META_ID}/runs?projectId={PROJECT_ID}",
    f"/v1/workflows/{WORKFLOW_META_ID}/batches/{SCHEDULE_META_ID}/runs?projectId={PROJECT_ID}",
    f"/v1/workflows/batch/{SCHEDULE_META_ID}/runs?projectId={PROJECT_ID}",
    f"/v1/workflows/batches/{SCHEDULE_META_ID}/runs?projectId={PROJECT_ID}",
    f"/v1/workflows/{WORKFLOW_META_ID}/schedules/{SCHEDULE_META_ID}/batch-runs?projectId={PROJECT_ID}",
    f"/v1/workflows/{WORKFLOW_META_ID}/schedules/{SCHEDULE_META_ID}/job-runs?projectId={PROJECT_ID}",
    f"/v1/workflows/{WORKFLOW_META_ID}/schedules/{SCHEDULE_META_ID}/executions?projectId={PROJECT_ID}",
    # Maybe it uses batchId (88538), not scheduleMetaId
    f"/v1/workflows/{WORKFLOW_META_ID}/batches/{BATCH_ID}/runs?projectId={PROJECT_ID}",
    f"/v1/workflows/batches/{BATCH_ID}?projectId={PROJECT_ID}",
    f"/v1/workflows/batches/{BATCH_ID}/runs?projectId={PROJECT_ID}",
]:
    get(f"{WF}{path}", path)

print("\n" + "=" * 70)
print("SEARCHING JS FOR BATCH RUN ENDPOINTS")
print("=" * 70)

r = requests.get(f"{MAIN}/element/static/js/main.7c45117e.js",
                 headers=headers_get, cookies=cookies, verify=False, timeout=60)
js = r.text

# Search for BATCH-specific API patterns
for term in ["BATCH", "batch-run", "batchRun", "batch_run", "getLastRun", "lastRun", "latestRun",
             "fetchLatestSchedules", "fetchAllSchedules", "runHistory", "node-run", "dag-run"]:
    idx = 0
    count = 0
    while count < 3:
        pos = js.find(term, idx)
        if pos == -1:
            break
        ctx = js[max(0, pos-200):pos+400]
        # Look for API call patterns
        if any(kw in ctx for kw in ["31200", "workflows", "schedules", "/v1/", "concat(", "fetch", "axios"]):
            count += 1
            print(f"\n  ✅ '{term}' at pos {pos}: ...{ctx}...")
        idx = pos + len(term)
    if count == 0 and js.find(term) != -1:
        pos = js.find(term)
        print(f"  ℹ️  '{term}' found at pos {pos} but not near API call")
    elif js.find(term) == -1:
        print(f"  ✗ '{term}' not found")

# Also look for the function that fetches the schedule's run details
print("\n" + "=" * 70)
print("SEARCHING FOR fetchLatestSchedules IMPLEMENTATION")
print("=" * 70)
term = "fetchLatestSchedules"
pos = js.find(term)
while pos != -1:
    ctx = js[max(0, pos-50):pos+600]
    if "concat" in ctx or "31200" in ctx or "function" in ctx:
        print(f"\n  pos={pos}: ...{ctx}...")
    pos = js.find(term, pos + len(term))

# Look for how the run table populates with run IDs
print("\n" + "=" * 70)
print("SEARCHING FOR getLastRunInstan (from previous probe truncation)")
print("=" * 70)
for term in ["getLastRunInstan", "getLastRun", "lastRunInstance", "RunInstance"]:
    pos = js.find(term)
    if pos != -1:
        ctx = js[max(0, pos-200):pos+600]
        print(f"\n  ✅ '{term}' at pos {pos}: ...{ctx}...")
    else:
        print(f"  ✗ '{term}' not found")

# Check what endpoints are called when viewing job history for BATCH type
print("\n" + "=" * 70)
print("SEARCHING FOR /v1/workflow/ (singular) PATHS")
print("=" * 70)
# The API config has both workflows and workflow (singular)
for term in ["/v1/workflow/", "workflow/batch", "workflow/run", "workflow-run"]:
    idx = 0
    count = 0
    while count < 5:
        pos = js.find(term, idx)
        if pos == -1:
            break
        count += 1
        ctx = js[max(0, pos-200):pos+400]
        print(f"\n  [{count}] '{term}' at pos {pos}: ...{ctx}...")
        idx = pos + len(term)
    if count == 0:
        print(f"  ✗ '{term}' not found")

# Look for where fetchJobsDeleteStatus is implemented (might show job list endpoint)
print("\n" + "=" * 70)
print("SEARCHING FOR fetchJobsDeleteStatus / fetchAllSchedules IMPLEMENTATIONS")
print("=" * 70)
for fname in ["fetchJobsDeleteStatus", "fetchAllSchedules", "fetchWorkflowSchedule"]:
    pos = js.find(fname)
    while pos != -1:
        ctx = js[max(0, pos-100):pos+600]
        if "concat" in ctx or "yield" in ctx:
            print(f"\n  [{fname}] pos={pos}: ...{ctx}...")
            break
        pos = js.find(fname, pos + len(fname))

print("\nDone.")
