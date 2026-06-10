"""
probe_full_history.py — Find the parameter combination that returns ALL schedule runs
(including June 2026 data). Currently both _fetch({}) and _fetch({activeOnly:false})
return the same April 28 - May 27 pool. We need June data too.

Run: python probe_full_history.py
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
API_BASE = "https://ml.prod.walmart.com:31200"
PROJECT_ID = 16701

def fetch(params, label):
    r = requests.get(
        f"{API_BASE}/v1/workflows/schedules",
        params={"projectId": PROJECT_ID, **params},
        headers=headers, cookies=cookies, verify=False, timeout=20,
    )
    if r.status_code != 200:
        print(f"  [{label}] HTTP {r.status_code}: {r.text[:200]}")
        return []
    data = r.json()
    if isinstance(data, list):
        runs = data
    else:
        runs = (data.get("schedules") or data.get("data") or data.get("result") or [])
        # Print top-level keys to understand response structure
        print(f"  [{label}] response keys: {list(data.keys())[:10]}")
    dates = sorted(set(str(r.get("scheduleStartDate",""))[:10] for r in runs if r.get("scheduleStartDate")))
    statuses = {}
    for r in runs:
        s = r.get("scheduleStatus","?")
        statuses[s] = statuses.get(s, 0) + 1
    print(f"  [{label}] count={len(runs)} dates={dates[0] if dates else 'none'}→{dates[-1] if dates else 'none'} statuses={statuses}")
    return runs

print("=" * 70)
print(f"Probing /v1/workflows/schedules for project {PROJECT_ID}")
print("=" * 70)

# Baseline
print("\n--- Baseline ---")
fetch({}, "no params")
fetch({"activeOnly": "false"}, "activeOnly=false")
fetch({"activeOnly": "true"}, "activeOnly=true")
fetch({"activeOnly": "false", "archived": "false"}, "activeOnly=false,archived=false")
fetch({"activeOnly": "false", "archived": "true"}, "activeOnly=false,archived=true")

# Status filters
print("\n--- Status filters ---")
for status in ("FAILED", "COMPLETED", "RUNNING", "ALL"):
    fetch({"status": status}, f"status={status}")
    fetch({"scheduleStatus": status}, f"scheduleStatus={status}")

# Pagination — maybe the API has pages and "recent" is page 0
print("\n--- Pagination ---")
fetch({"page": 0, "size": 100}, "page=0,size=100")
fetch({"page": 0, "size": 200}, "page=0,size=200")
fetch({"page": 1, "size": 100}, "page=1,size=100")
fetch({"pageNo": 0, "pageSize": 100}, "pageNo=0,pageSize=100")
fetch({"pageNumber": 0, "pageSize": 100}, "pageNumber=0,pageSize=100")
fetch({"limit": 200}, "limit=200 only")
fetch({"limit": 200, "activeOnly": "false"}, "limit=200,activeOnly=false")

# Date range filters
print("\n--- Date range ---")
fetch({"startDate": "2026-05-28", "endDate": "2026-06-10"}, "startDate/endDate June")
fetch({"fromDate": "2026-05-28", "toDate": "2026-06-10"}, "fromDate/toDate June")
fetch({"scheduleStartDateFrom": "2026-05-28"}, "scheduleStartDateFrom")
fetch({"dateFrom": "2026-05-28", "dateTo": "2026-06-10"}, "dateFrom/dateTo")

# Check if the API has a "total" field that tells us how many records exist
print("\n--- Inspect raw response structure ---")
r = requests.get(
    f"{API_BASE}/v1/workflows/schedules",
    params={"projectId": PROJECT_ID},
    headers=headers, cookies=cookies, verify=False, timeout=20,
)
raw = r.json()
if isinstance(raw, dict):
    print(f"Top-level keys: {list(raw.keys())}")
    for k in ("total", "totalCount", "count", "totalElements", "totalPages"):
        if k in raw:
            print(f"  {k} = {raw[k]}")
    # Show first record to understand structure
    sample = (raw.get("schedules") or raw.get("data") or raw.get("result") or [])
    if sample:
        print(f"\nSample record keys: {list(sample[0].keys())}")
        print(f"Sample record: {json.dumps(sample[0])[:600]}")
else:
    print(f"Response is a list of {len(raw)} items")
    if raw:
        print(f"Sample record keys: {list(raw[0].keys())}")

# Try alternative endpoint patterns
print("\n--- Alternative endpoints ---")
for path in (
    f"/v1/workflows/schedules/history?projectId={PROJECT_ID}",
    f"/v1/workflows/schedules/all?projectId={PROJECT_ID}",
    f"/v1/projects/{PROJECT_ID}/schedules",
    f"/v1/schedules?projectId={PROJECT_ID}",
):
    r2 = requests.get(f"{API_BASE}{path}", headers=headers, cookies=cookies, verify=False, timeout=15)
    print(f"  GET {path} → [{r2.status_code}] {r2.text[:200]}")

print("\nDone.")
