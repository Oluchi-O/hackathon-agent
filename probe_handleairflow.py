"""
probe_handleairflow.py — Find handleAirflow() and the Airflow URL pattern it navigates to.

JS: "dagRun"/"taskRun" → this.handleAirflow(e, o, a) where a=dagId, o=scheduleData
This navigates to the Airflow UI page which shows dag run history.
The Airflow URL contains the dagRunId which includes the spark_job_id.
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
DAG_ID = "dag_48227_fe957158-dff8-43c0-ae7c-813cb13f5dab"
BATCH_ID = "88538"

print("=" * 70)
print("SEARCHING JS: handleAirflow function")
print("=" * 70)

r = requests.get(f"{MAIN}/element/static/js/main.7c45117e.js",
                 headers=headers, cookies=cookies, verify=False, timeout=60)
js = r.text

# Find handleAirflow
idx = 0
count = 0
while count < 5:
    pos = js.find("handleAirflow", idx)
    if pos == -1: break
    count += 1
    ctx = js[max(0, pos-200):pos+800]
    print(f"\n  [{count}] pos={pos}: ...{ctx}...")
    idx = pos + len("handleAirflow")

# Also search for astroUrl / fetchAstroUrl (Airflow UI URL)
print("\n" + "=" * 70)
print("SEARCHING JS: astroUrl / fetchAstroUrl (Airflow dashboard URL)")
print("=" * 70)
for term in ["astroUrl", "fetchAstroUrl", "AstroUrl", "astro_url", "airflowUrl", "airflow_url"]:
    idx = 0; count = 0
    while count < 3:
        pos = js.find(term, idx)
        if pos == -1: break
        ctx = js[max(0, pos-200):pos+600]
        if any(k in ctx for k in ["concat", "yield", "fetch", "T1", "dispatch"]):
            count += 1
            print(f"\n  [{count}] '{term}' pos={pos}: ...{ctx}...")
        idx = pos + len(term)
    if count == 0:
        total = js.count(term)
        if total > 0:
            pos = js.find(term)
            ctx = js[max(0, pos-100):pos+400]
            print(f"  ℹ️  '{term}' found {total}x: pos={pos}: ...{ctx}...")
        else:
            print(f"  ✗ '{term}' not found")

# Also search for fetchAstroUrl implementation
print("\n" + "=" * 70)
print("SEARCHING JS: fetchAstroUrl IMPLEMENTATION")
print("=" * 70)
idx = 0
while True:
    pos = js.find("fetchAstroUrl", idx)
    if pos == -1: break
    ctx = js[max(0, pos-100):pos+800]
    if "yield" in ctx or "T1(" in ctx:
        print(f"\n  pos={pos}: ...{ctx}...")
    idx = pos + len("fetchAstroUrl")

# The Airflow UI is accessed via an astro URL — let's also try to fetch it directly
print("\n" + "=" * 70)
print("FETCHING: Astro/Airflow URL from schedule data")
print("=" * 70)

# From schedule data response: workflowDagId, dagLocation
# Try the astroUrl endpoint
def get(url, label=""):
    try:
        r = requests.get(url, headers=headers, cookies=cookies, verify=False, timeout=20)
        label = label or url
        print(f"\n  [{r.status_code}] {label}")
        if r.status_code == 200:
            print(f"  ✅ SUCCESS! Body: {r.text[:3000]}")
        else:
            print(f"  Body: {r.text[:500]}")
        return r
    except Exception as e:
        print(f"\n  [ERR] {label}: {e}")
        return None

for path in [
    f"/v1/workflows/{WORKFLOW_META_ID}/schedules/{SCHEDULE_META_ID}/astro?projectId={PROJECT_ID}",
    f"/v1/workflows/schedules/{SCHEDULE_META_ID}/astro?projectId={PROJECT_ID}",
    f"/v1/workflows/schedules/{SCHEDULE_META_ID}/airflow-url?projectId={PROJECT_ID}",
    f"/v1/workflows/astro-url?dagId={DAG_ID}&projectId={PROJECT_ID}",
    f"/v1/workflows/astro?dagId={DAG_ID}&projectId={PROJECT_ID}",
    f"/v1/workflow/astro?dagId={DAG_ID}&scheduleId={SCHEDULE_META_ID}&projectId={PROJECT_ID}",
    # From m.airflow = :31206 — try via port 31200 with /airflow prefix
    f"/v1/airflow/dags/{DAG_ID}/dagRuns?projectId={PROJECT_ID}&limit=5",
    f"/v1/airflow/{DAG_ID}/dagRuns?projectId={PROJECT_ID}",
    f"/v1/airflow/dagRuns?dagId={DAG_ID}&projectId={PROJECT_ID}",
]:
    get(f"{WF}{path}", path)

print("\nDone.")
