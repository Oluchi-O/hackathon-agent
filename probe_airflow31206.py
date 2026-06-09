"""
probe_airflow31206.py — Try Airflow API at port 31206 (the real airflow port).

The API config map shows: airflow: "...o:31206"
All previous attempts used 31200. This tries 31206.
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
AIRFLOW = f"{MAIN}:31206"

# Known IDs
DAG_ID_FULL = "dag_48227_fe957158-dff8-43c0-ae7c-813cb13f5dab"
DAG_ID_SHORT = "dag_48227"
SCHEDULE_META_ID = "895958"
WORKFLOW_META_ID = "48227"
BATCH_ID = "88538"

def try_url(url, label=""):
    try:
        r = requests.get(url, headers=headers, cookies=cookies, verify=False, timeout=15)
        body = r.text[:1000]
        print(f"\n  [{r.status_code}] {label or url}")
        if r.status_code == 200:
            print(f"  ✅ SUCCESS! Body: {body}")
            # Try to find spark_job_id or ELEMENT_WORKFLOW_RUN_ID
            for key in ["spark_job", "ELEMENT_WORKFLOW", "run_id", "job_id", "jobRun", "sparkJob"]:
                if key in r.text:
                    idx = r.text.find(key)
                    print(f"  🔑 Found '{key}': ...{r.text[max(0,idx-50):idx+200]}...")
        else:
            print(f"  Body: {body}")
        return r
    except Exception as e:
        print(f"\n  [ERR] {label or url}: {e}")
        return None

print("=" * 70)
print(f"PROBING AIRFLOW AT PORT 31206")
print("=" * 70)

# 1. Airflow health / version
for path in ["/health", "/api/v1/health", "/api/v1/version", "/api/v1/config"]:
    try_url(f"{AIRFLOW}{path}", f"31206{path}")

# 2. DAG list
try_url(f"{AIRFLOW}/api/v1/dags?limit=5", "31206 DAG list")

# 3. DAG runs for our specific DAG
try_url(f"{AIRFLOW}/api/v1/dags/{DAG_ID_FULL}/dagRuns?limit=5", "31206 dagRuns full id")
try_url(f"{AIRFLOW}/api/v1/dags/{DAG_ID_SHORT}/dagRuns?limit=5", "31206 dagRuns short id")

# 4. Task instances (could have spark_job_id in xcom)
try_url(f"{AIRFLOW}/api/v1/dags/{DAG_ID_FULL}/dagRuns/~/taskInstances?limit=5", "31206 taskInstances")

# 5. XCom values
try_url(f"{AIRFLOW}/api/v1/dags/{DAG_ID_FULL}/dagRuns/~/taskInstances/~/xcomEntries?limit=10", "31206 xcom all")

print("\n" + "=" * 70)
print("PROBING PORT 31200 WORKFLOW ENDPOINT FOR SPARK_JOB_ID")
print("=" * 70)
WF = f"{MAIN}:31200"

# workflow schedules endpoint
for path in [
    f"/v1/workflows/schedules/{SCHEDULE_META_ID}",
    f"/v1/workflows/schedules/{SCHEDULE_META_ID}/runs",
    f"/v1/workflows/schedules/{SCHEDULE_META_ID}/runs?limit=5",
    f"/v1/workflows/{WORKFLOW_META_ID}/schedules/{SCHEDULE_META_ID}/runs",
    f"/v1/workflow/schedules/{SCHEDULE_META_ID}/runs",
    f"/v1/workflows/schedule/{SCHEDULE_META_ID}/runs",
    f"/v1/workflows/runs?scheduleId={SCHEDULE_META_ID}",
    f"/v1/workflows/runs?scheduleMetaId={SCHEDULE_META_ID}",
    f"/v1/workflows/{WORKFLOW_META_ID}/runs",
    f"/v1/workflows/{WORKFLOW_META_ID}/runs?limit=5",
]:
    try_url(f"{WF}{path}", f"31200 {path}")

print("\n" + "=" * 70)
print("PROBING PORT 31001 NOTEBOOK PATHS (using m.notebooks base)")
print("=" * 70)
NB = f"{MAIN}:31001"

for path in [
    f"/v1/jobs",
    f"/v1/jobs?limit=5",
    f"/v1/jobs?batchId={BATCH_ID}",
    f"/v1/batches",
    f"/v1/batches/{BATCH_ID}",
    f"/v1/batches/{BATCH_ID}/runs",
    f"/v1/batches/{BATCH_ID}/runs?limit=5",
    f"/v1/sessions",
    f"/v1/session",
]:
    try_url(f"{NB}{path}", f"31001 {path}")

print("\nDone.")
