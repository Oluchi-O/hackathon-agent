"""
probe_fetchHistoricSchedules.py
Find the fetchHistoricSchedules (GF) implementation URL in the deployments JS bundle.

Known:
  - exports at pos ~97652 in a second JS chunk: fetchHistoricSchedules:()=>GF
  - call site: GF({projectId, targetId, scheduleId})
  - scheduleId corresponds to scheduleMetaId (895958)
  - targetId is unknown
"""
import requests, re, sys, os, urllib3
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

headers = {"Authorization": f"Bearer {BEARER}", "Accept": "text/html,*/*"}
MAIN = "https://ml.prod.walmart.com"

# ── Fetch SPA shell to get all JS chunk URLs ──────────────────────────────────
print("Fetching main app HTML...")
r = requests.get(f"{MAIN}/element/", headers=headers, cookies=cookies,
                 verify=False, timeout=15, allow_redirects=True)
html = r.text
print(f"Status: {r.status_code} | size: {len(html)} chars")

scripts = re.findall(r'src=["\']([^"\']+\.js[^"\']*)["\']', html)
chunks = re.findall(r'["\']([^"\']+\.[a-f0-9]{8}\.chunk\.js)["\']', html)
# Also grab lazy chunks embedded in the main bundle
all_scripts = list(dict.fromkeys(scripts + chunks))
print(f"Found {len(all_scripts)} JS files in shell")

# ── Search each bundle for the GF implementation ─────────────────────────────
SEARCH_TERMS = [
    "fetchHistoricSchedules",
    "function*GF",
    "GF=function",
    "historicSchedules",
]

for s in all_scripts:
    url = s if s.startswith("http") else f"{MAIN}/{s.lstrip('/')}"
    try:
        jr = requests.get(url, headers=headers, cookies=cookies,
                          verify=False, timeout=30)
        content = jr.text
        # Only deeply analyse bundles that contain fetchHistoricSchedules
        if "fetchHistoricSchedules" not in content:
            continue
        print(f"\n{'='*70}")
        print(f"FOUND in bundle: {url}")
        print(f"Size: {len(content)} chars | status {jr.status_code}")
        print(f"{'='*70}")

        for term in SEARCH_TERMS:
            # Find ALL occurrences
            start = 0
            count = 0
            while True:
                idx = content.find(term, start)
                if idx == -1:
                    break
                ctx = content[max(0, idx-200):idx+400]
                count += 1
                print(f"\n  [{count}] '{term}' at pos {idx}:")
                print(f"  ...{ctx}...")
                start = idx + 1
                if count >= 5:
                    print(f"  (more occurrences suppressed)")
                    break

        # Also search for the URL pattern — look for /schedules/ near historicSchedules
        for api_hint in ["/schedules/", "historic", "schedule-history", "scheduleHistory",
                         "runHistory", "run-history", "/runs", "/history"]:
            idx = content.find(api_hint)
            if idx != -1 and "fetchHistoric" in content[max(0, idx-500):idx+500]:
                ctx = content[max(0, idx-300):idx+300]
                print(f"\n  [API HINT] '{api_hint}' near fetchHistoric:")
                print(f"  ...{ctx}...")

    except Exception as e:
        print(f"  ERR {url}: {e}")

# ── Also probe endpoint candidates directly ──────────────────────────────────
print("\n" + "="*70)
print("PROBING endpoint candidates with scheduleMetaId=895958")
print("="*70)

API_BASE = "https://ml.prod.walmart.com:31200"
PROJECT_ID = "16701"
SCHEDULE_ID = "895958"
WORKFLOW_ID = "48227"

api_headers = {"Authorization": f"Bearer {BEARER}",
               "Accept": "application/json"}

for path in [
    f"/v1/workflows/schedules/{SCHEDULE_ID}/history?projectId={PROJECT_ID}",
    f"/v1/workflows/schedules/{SCHEDULE_ID}/historic?projectId={PROJECT_ID}",
    f"/v1/workflows/{WORKFLOW_ID}/schedules/{SCHEDULE_ID}/history?projectId={PROJECT_ID}",
    f"/v1/deployments/{SCHEDULE_ID}/historicSchedules?projectId={PROJECT_ID}",
    f"/v1/deployments/schedules/{SCHEDULE_ID}/history?projectId={PROJECT_ID}",
    f"/v1/mcDeployments/{SCHEDULE_ID}/schedules?projectId={PROJECT_ID}",
    f"/v1/workflows/schedules/{SCHEDULE_ID}/schedule-history?projectId={PROJECT_ID}",
    f"/v1/workflows/{WORKFLOW_ID}/schedules/history?projectId={PROJECT_ID}",
    f"/v1/workflows/schedules/history?scheduleId={SCHEDULE_ID}&projectId={PROJECT_ID}",
]:
    try:
        rr = requests.get(f"{API_BASE}{path}", headers=api_headers, cookies=cookies,
                          verify=False, timeout=15)
        status_icon = "✅" if rr.status_code == 200 else "❌"
        print(f"\n  {status_icon} [{rr.status_code}] {path}")
        print(f"  Body[:400]: {rr.text[:400]}")
    except Exception as e:
        print(f"\n  [ERR] {path}: {e}")

print("\nDone.")
