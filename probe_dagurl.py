"""
probe_dagurl.py — Call the correct dagUrl endpoint.

From JS (fetchAtroUrl with typo):
  GET /v1/workflows/dag/{dagId}/dagUrl?type={task}
Returns astroUrl.url = Airflow UI URL (e.g. Astronomer/Airflow hosted URL)

Then use that URL to access the Airflow REST API.
"""
import requests, json, os, re, urllib3
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

DAG_ID = "dag_48227_fe957158-dff8-43c0-ae7c-813cb13f5dab"
DAG_ID_SHORT = "dag_48227"
SCHEDULE_META_ID = "895958"
WORKFLOW_META_ID = "48227"
PROJECT_ID = "16701"

def get(url, label="", extra_headers=None):
    try:
        h = {**headers, **(extra_headers or {})}
        r = requests.get(url, headers=h, cookies=cookies, verify=False, timeout=30)
        label = label or url
        print(f"\n  [{r.status_code}] {label}")
        if r.status_code == 200:
            print(f"  ✅ SUCCESS!")
            try:
                data = r.json()
                print(json.dumps(data, indent=2)[:5000])
            except:
                print(r.text[:3000])
        else:
            print(f"  Body: {r.text[:600]}")
        return r
    except Exception as e:
        print(f"\n  [ERR] {label}: {e}")
        return None

print("=" * 70)
print("CORRECT ENDPOINT: GET /v1/workflows/dag/{dagId}/dagUrl?type={task}")
print("=" * 70)

airflow_url = None
for task in ["dagRun", "taskRun", "showDag"]:
    r = get(f"{WF}/v1/workflows/dag/{DAG_ID}/dagUrl?type={task}",
            f"/v1/workflows/dag/{DAG_ID}/dagUrl?type={task}")
    if r and r.status_code == 200:
        try:
            data = r.json()
            # Extract the URL from the response
            url = data.get("url") or data.get("airflowUrl") or data.get("astroUrl") or str(data)
            print(f"  🔑 Airflow URL: {url}")
            if airflow_url is None and "http" in str(url):
                airflow_url = url
        except:
            pass

# Also try short dag id
for task in ["dagRun"]:
    r = get(f"{WF}/v1/workflows/dag/{DAG_ID_SHORT}/dagUrl?type={task}",
            f"/v1/workflows/dag/{DAG_ID_SHORT}/dagUrl?type={task}")

print("\n" + "=" * 70)
print("If we got an Airflow URL, query its REST API for dag runs")
print("=" * 70)

if airflow_url:
    # Parse the host from the Airflow URL
    print(f"Airflow URL: {airflow_url}")
    # Try to extract the base URL
    match = re.match(r'(https?://[^/]+)', str(airflow_url))
    if match:
        airflow_base = match.group(1)
        print(f"Airflow base: {airflow_base}")

        # Try Airflow REST API
        for path in [
            f"/api/v1/dags/{DAG_ID}/dagRuns?limit=5&order_by=-execution_date",
            f"/api/v1/dags/{DAG_ID_SHORT}/dagRuns?limit=5",
            f"/api/v1/health",
        ]:
            get(f"{airflow_base}{path}", f"airflow{path}")
else:
    print("  No Airflow URL found yet. Trying alternative endpoints...")

    # Try port 31200 with different path formats
    for path in [
        f"/v1/workflows/dag/{DAG_ID}/dagUrl",
        f"/v1/workflows/dags/{DAG_ID}/url?type=dagRun",
        f"/v1/workflows/dag/{DAG_ID}/url?type=dagRun",
        f"/v1/workflow/dag/{DAG_ID}/dagUrl?type=dagRun",
        f"/v1/workflows/airflow/dag/{DAG_ID}/dagUrl?type=dagRun",
        # Try with numeric workflowMetaId instead of dagId
        f"/v1/workflows/{WORKFLOW_META_ID}/dag/dagUrl?type=dagRun",
        f"/v1/workflows/{WORKFLOW_META_ID}/dagUrl?type=dagRun",
        f"/v1/workflows/{WORKFLOW_META_ID}/dag-url?type=dagRun&dagId={DAG_ID}",
    ]:
        get(f"{WF}{path}", path)

print("\nDone.")
