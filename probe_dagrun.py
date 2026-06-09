"""
probe_dagrun.py — Find how "dagRun" and "taskRun" actions work for BATCH type jobs.

From JS: BATCH menu has "Workflow Instances" (value:"dagRun") and "Node Instances" (value:"taskRun").
These are NOT hidden for BATCH type — they must call some API to get run info.
Also try the Airflow endpoint at port 31206 with the dagId.
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
AIRFLOW = f"{MAIN}:31206"

PROJECT_ID = "16701"
WORKFLOW_META_ID = "48227"
SCHEDULE_META_ID = "895958"
BATCH_ID = "88538"
DAG_ID = "dag_48227_fe957158-dff8-43c0-ae7c-813cb13f5dab"
DAG_ID_SHORT = "dag_48227"

def get(url, label=""):
    try:
        r = requests.get(url, headers=headers, cookies=cookies, verify=False, timeout=20)
        label = label or url
        print(f"\n  [{r.status_code}] {label}")
        if r.status_code == 200:
            print(f"  ✅ SUCCESS!")
            try:
                data = r.json()
                text = json.dumps(data, indent=2)[:5000]
            except:
                text = r.text[:3000]
            print(text)
            # Check for spark_job_id
            for key in ["spark_job", "ELEMENT_WORKFLOW", "run_id", "jobRun", "sparkJob"]:
                if key in r.text:
                    idx = r.text.find(key)
                    print(f"  🔑 '{key}': ...{r.text[max(0,idx-30):idx+300]}...")
        else:
            print(f"  Body: {r.text[:600]}")
        return r
    except Exception as e:
        print(f"\n  [ERR] {label}: {e}")
        return None

print("=" * 70)
print("SEARCHING JS: dagRun / taskRun action handlers")
print("=" * 70)

r = requests.get(f"{MAIN}/element/static/js/main.7c45117e.js",
                 headers=headers, cookies=cookies, verify=False, timeout=60)
js = r.text

# Find action handler for dagRun and taskRun
for term in ['"dagRun"', '"taskRun"', "dagRun", "taskRun"]:
    idx = 0; count = 0
    while count < 5:
        pos = js.find(term, idx)
        if pos == -1: break
        ctx = js[max(0, pos-300):pos+500]
        if any(k in ctx for k in ["action", "dispatch", "props", "schedule", "workflow", "fetch", "api", "concat"]):
            count += 1
            print(f"\n  [{count}] '{term}' pos={pos}: ...{ctx}...")
        idx = pos + len(term)
    if count == 0:
        total = js.count(term)
        print(f"  ℹ️  '{term}' found {total}x but not near action context")

print("\n" + "=" * 70)
print("ALSO: Airflow endpoint paths from the config (m.airflow = :31206)")
print("Port 31206 returns nginx 404 directly — check if it's proxied differently")
print("=" * 70)

# The airflow key points to port 31206, but nginx returns 404
# Maybe the airflow API goes through a different path, like /airflow/ or /api/airflow/
for base in [f"{MAIN}:31206", f"{MAIN}:31200"]:
    for path in [
        f"/airflow/api/v1/health",
        f"/airflow/api/v1/dags?limit=3",
        f"/airflow/api/v1/dags/{DAG_ID}/dagRuns?limit=5",
        f"/api/v1/dags/{DAG_ID}/dagRuns?limit=5",
        f"/airflow/health",
        f"/airflow/api/v1/dags",
    ]:
        get(f"{base}{path}", f"{base.split(':')[-1]}{path}")

print("\n" + "=" * 70)
print("FETCHING BATCH RUN HISTORY: trying dagId-based endpoints on port 31200")
print("=" * 70)

for path in [
    f"/v1/workflows/dag-runs?dagId={DAG_ID}&projectId={PROJECT_ID}",
    f"/v1/workflows/dag-runs?dagId={DAG_ID_SHORT}&projectId={PROJECT_ID}",
    f"/v1/workflows/dag-runs/{DAG_ID}?projectId={PROJECT_ID}",
    f"/v1/workflows/{WORKFLOW_META_ID}/dag-runs?projectId={PROJECT_ID}",
    f"/v1/workflows/{WORKFLOW_META_ID}/dag-runs?projectId={PROJECT_ID}&scheduleId={SCHEDULE_META_ID}",
    f"/v1/workflow/dag-runs?dagId={DAG_ID}&projectId={PROJECT_ID}",
    f"/v1/workflow/dag-runs/{DAG_ID}?projectId={PROJECT_ID}",
    f"/v1/workflows/{WORKFLOW_META_ID}/schedules/{SCHEDULE_META_ID}/dag-runs?projectId={PROJECT_ID}",
    # Try "dagRuns" instead of "dag-runs"
    f"/v1/workflows/dagRuns?dagId={DAG_ID}&projectId={PROJECT_ID}",
    f"/v1/workflows/{WORKFLOW_META_ID}/dagRuns?projectId={PROJECT_ID}",
]:
    get(f"{WF}{path}", path)

print("\nDone.")
