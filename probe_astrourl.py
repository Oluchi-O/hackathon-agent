"""
probe_astrourl.py — Call fetchAstroUrl endpoint and extract Airflow host URL.

From JS: fetchAstroUrl({dagId, task}) calls:
  GET /v1/workflows/{dagId}?type={task}
Returns astroUrl.url = the Airflow UI URL (embedded in iframe).
Once we have the Airflow host, we can query its REST API for dag runs + XCom.
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

DAG_ID = "dag_48227_fe957158-dff8-43c0-ae7c-813cb13f5dab"
DAG_ID_SHORT = "dag_48227"
SCHEDULE_META_ID = "895958"
WORKFLOW_META_ID = "48227"
PROJECT_ID = "16701"
BATCH_ID = "88538"

def get(url, label=""):
    try:
        r = requests.get(url, headers=headers, cookies=cookies, verify=False, timeout=20)
        label = label or url
        print(f"\n  [{r.status_code}] {label}")
        if r.status_code == 200:
            print(f"  ✅ SUCCESS!")
            try:
                data = r.json()
                print(json.dumps(data, indent=2)[:5000])
            except:
                print(r.text[:3000])
            for key in ["spark_job", "ELEMENT_WORKFLOW", "run_id", "dagRunId", "airflow", "url", "href"]:
                if key.lower() in r.text.lower():
                    idx = r.text.lower().find(key.lower())
                    print(f"  🔑 '{key}': ...{r.text[max(0,idx-20):idx+200]}...")
        else:
            print(f"  Body: {r.text[:600]}")
        return r
    except Exception as e:
        print(f"\n  [ERR] {label}: {e}")
        return None

print("=" * 70)
print("CALLING fetchAstroUrl PATTERN: GET /v1/workflows/{dagId}?type={task}")
print("=" * 70)

# The JS calls: l.Ay.workflows + "/" + dagId + "?type=" + task
# l.Ay.workflows = "https://ml.prod.walmart.com:31200/v1/workflows"
# So: GET /v1/workflows/{dagId}?type=dagRun

for task in ["dagRun", "taskRun", "showDag"]:
    for dag in [DAG_ID, DAG_ID_SHORT]:
        get(f"{WF}/v1/workflows/{dag}?type={task}", f"/v1/workflows/{dag}?type={task}")

# Also try without type param
get(f"{WF}/v1/workflows/{DAG_ID}", f"/v1/workflows/{DAG_ID} (no params)")

print("\n" + "=" * 70)
print("ALSO: fetchAstroUrl context more carefully from JS")
print("=" * 70)

r = requests.get(f"{MAIN}/element/static/js/main.7c45117e.js",
                 headers=headers, cookies=cookies, verify=False, timeout=60)
js = r.text

# Find the full fetchAstroUrl implementation
print("Searching for full fetchAstroUrl implementation...")
idx = 0
while True:
    pos = js.find("fetchAtroUrl", idx)  # Note: there's a typo in the JS "fetchAtroUrl" (missing 's')
    if pos == -1: break
    ctx = js[max(0, pos-100):pos+1000]
    print(f"\n  pos={pos}: ...{ctx}...")
    idx = pos + 12

# Also find handleAirflow function
print("\nSearching handleAirflow function implementation...")
idx = 0
while True:
    pos = js.find("handleAirflow", idx)
    if pos == -1: break
    ctx = js[max(0, pos-50):pos+1200]
    if "function" in ctx or "=>" in ctx or "{" in ctx:
        print(f"\n  pos={pos}: ...{ctx}...")
    idx = pos + len("handleAirflow")

print("\nDone.")
