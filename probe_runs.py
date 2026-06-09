"""
probe_runs.py — Target the endpoints that returned 405/500 (meaning they exist).

Key findings from previous probe:
- /v1/workflows/48227/schedules/895958/runs → 500 "projectId required" → endpoint EXISTS
- /v1/workflows/schedules/895958/runs → 405 Method Not Allowed → endpoint EXISTS

Also search JS for how runDetails is fetched.
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

headers_json = {"Authorization": f"Bearer {BEARER}", "Accept": "application/json", "Content-Type": "application/json"}
headers_get = {"Authorization": f"Bearer {BEARER}", "Accept": "application/json"}
MAIN = "https://ml.prod.walmart.com"
WF = f"{MAIN}:31200"

# Known IDs
PROJECT_ID = "16701"
WORKFLOW_META_ID = "48227"
SCHEDULE_META_ID = "895958"
BATCH_ID = "88538"
DAG_ID_FULL = "dag_48227_fe957158-dff8-43c0-ae7c-813cb13f5dab"

def get(url, label=""):
    try:
        r = requests.get(url, headers=headers_get, cookies=cookies, verify=False, timeout=15)
        body = r.text[:2000]
        label = label or url
        print(f"\n  [{r.status_code}] {label}")
        if r.status_code == 200:
            print(f"  ✅ SUCCESS!")
            print(f"  Body: {body}")
            for key in ["spark_job", "ELEMENT_WORKFLOW", "run_id", "jobRun", "sparkJob", "batchId", "htmlUrl", "notebookUrl"]:
                if key in r.text:
                    idx = r.text.find(key)
                    print(f"  🔑 '{key}': ...{r.text[max(0,idx-30):idx+200]}...")
        else:
            print(f"  Body: {body}")
        return r
    except Exception as e:
        print(f"\n  [ERR] {label or url}: {e}")
        return None

def post(url, payload, label=""):
    try:
        r = requests.post(url, headers=headers_json, cookies=cookies, json=payload, verify=False, timeout=15)
        body = r.text[:2000]
        label = label or url
        print(f"\n  [{r.status_code}] POST {label}")
        if r.status_code in (200, 201):
            print(f"  ✅ SUCCESS! Body: {body}")
        else:
            print(f"  Body: {body}")
        return r
    except Exception as e:
        print(f"\n  [ERR] POST {label}: {e}")
        return None

print("=" * 70)
print("ENDPOINT EXISTS: /v1/workflows/{wfId}/schedules/{sId}/runs (needs projectId)")
print("=" * 70)

# Try with projectId query param
for path in [
    f"/v1/workflows/{WORKFLOW_META_ID}/schedules/{SCHEDULE_META_ID}/runs?projectId={PROJECT_ID}",
    f"/v1/workflows/{WORKFLOW_META_ID}/schedules/{SCHEDULE_META_ID}/runs?projectId={PROJECT_ID}&limit=5",
    f"/v1/workflows/{WORKFLOW_META_ID}/schedules/{SCHEDULE_META_ID}/runs?projectId={PROJECT_ID}&page=0&size=5",
]:
    get(f"{WF}{path}", path)

print("\n" + "=" * 70)
print("ENDPOINT EXISTS: /v1/workflows/schedules/{sId}/runs (405 → try POST)")
print("=" * 70)

# The 405 means GET not allowed → try POST
post(f"{WF}/v1/workflows/schedules/{SCHEDULE_META_ID}/runs",
     {"projectId": int(PROJECT_ID)},
     f"/v1/workflows/schedules/{SCHEDULE_META_ID}/runs (POST projectId)")

post(f"{WF}/v1/workflows/schedules/{SCHEDULE_META_ID}/runs",
     {"projectId": int(PROJECT_ID), "workflowId": int(WORKFLOW_META_ID)},
     f"/v1/workflows/schedules/{SCHEDULE_META_ID}/runs (POST both ids)")

# Also try HEAD to understand the 405
try:
    r = requests.options(f"{WF}/v1/workflows/schedules/{SCHEDULE_META_ID}/runs",
                         headers=headers_get, cookies=cookies, verify=False, timeout=10)
    print(f"\n  OPTIONS /v1/workflows/schedules/{SCHEDULE_META_ID}/runs → {r.status_code}")
    print(f"  Allow: {r.headers.get('Allow', 'not in headers')}")
except Exception as e:
    print(f"  OPTIONS error: {e}")

print("\n" + "=" * 70)
print("SEARCHING JS: how runDetails is fetched / what API call populates it")
print("=" * 70)

# Download JS and search
r = requests.get(f"{MAIN}/element/static/js/main.7c45117e.js",
                 headers=headers_get, cookies=cookies, verify=False, timeout=60)
js = r.text

# Search for the API call that fetches runDetails
for term in ["runDetails", "run-details", "run_details", "getRunDetails", "fetchRun", "getRuns"]:
    idx = 0
    count = 0
    while count < 3:
        pos = js.find(term, idx)
        if pos == -1:
            break
        ctx = js[max(0, pos-300):pos+400]
        # Only show if near an API call pattern
        if "fetch" in ctx.lower() or "axios" in ctx.lower() or "http" in ctx.lower() or "request" in ctx.lower() or ".get(" in ctx or "schedules" in ctx or "workflows" in ctx:
            count += 1
            print(f"\n  [{term}] pos={pos}: ...{ctx}...")
        idx = pos + len(term)

# Search for the actual schedule data fetch
print("\n" + "-" * 50)
print("Searching for scheduleId API calls:")
for term in ["scheduleId", "schedule_id"]:
    idx = 0
    count = 0
    while count < 5:
        pos = js.find(f'"{term}"', idx)
        if pos == -1:
            break
        ctx = js[max(0, pos-200):pos+300]
        if "get" in ctx.lower() or "fetch" in ctx.lower() or "31200" in ctx or "workflows" in ctx:
            count += 1
            print(f"\n  ['{term}'] pos={pos}: ...{ctx}...")
        idx = pos + len(term) + 2

# Look for the runDetails API handler
print("\n" + "-" * 50)
print("Searching for workflows node-runs or dag-runs API:")
for term in ["node-runs", "dag-runs", "dagRuns", "node_runs", "dag_runs", "runHistory", "run_history", "executionHistory"]:
    pos = js.find(term)
    if pos != -1:
        ctx = js[max(0, pos-300):pos+400]
        print(f"\n  ✅ '{term}' at pos {pos}: ...{ctx}...")
    else:
        print(f"  ✗ '{term}' not found")

print("\nDone.")
