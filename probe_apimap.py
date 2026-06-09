"""
probe_apimap.py — Extract the full API config object from main.7c45117e.js.

The object at pos ~5395 contains all endpoint URLs. The previous probe
truncated it. This script extracts a larger context to see all keys.
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

headers = {"Authorization": f"Bearer {BEARER}", "Accept": "*/*"}
MAIN = "https://ml.prod.walmart.com"

print("Fetching main.7c45117e.js...")
r = requests.get(f"{MAIN}/element/static/js/main.7c45117e.js",
                 headers=headers, cookies=cookies, verify=False, timeout=60)
js = r.text
print(f"Downloaded: {len(js)} chars\n")

# ── 1. Extract full API config object (pos ~5200–9000) ────────────────────────
print("=" * 70)
print("FULL API CONFIG OBJECT (pos 5200–9000):")
print(js[5200:9000])

# ── 2. Search for any port 31001 paths ────────────────────────────────────────
print("\n" + "=" * 70)
print("ALL OCCURRENCES OF ':31001' in first 50000 chars:")
idx = 0
count = 0
while count < 20:
    pos = js.find(":31001", idx)
    if pos == -1 or pos > 50000:
        break
    count += 1
    ctx = js[max(0, pos-200):pos+300]
    print(f"\n  [{count}] pos={pos}: ...{ctx}...")
    idx = pos + 6

# ── 3. Search for 'applications' endpoint ────────────────────────────────────
print("\n" + "=" * 70)
print("SEARCHING: 'applications' near port references:")
idx = 0
count = 0
while count < 10:
    pos = js.find("applications", idx)
    if pos == -1:
        break
    # Only show if near a port number
    ctx = js[max(0, pos-100):pos+200]
    if "31" in ctx or "port" in ctx.lower() or "concat" in ctx:
        count += 1
        print(f"\n  [{count}] pos={pos}: ...{ctx}...")
    idx = pos + 12

# ── 4. Search for 'sparkJob' / 'spark_job' ───────────────────────────────────
print("\n" + "=" * 70)
print("SEARCHING: sparkJob / spark_job / SparkJob:")
for term in ["sparkJob", "spark_job", "SparkJob", "spark-job", "elementRun", "element_run", "jobRun", "job_run"]:
    pos = js.find(term)
    if pos != -1:
        ctx = js[max(0, pos-200):pos+300]
        print(f"\n  ✅ '{term}' at pos {pos}: ...{ctx}...")
    else:
        print(f"  ✗ '{term}' not found")

# ── 5. Look at the View button / link in run history table ───────────────────
print("\n" + "=" * 70)
print("SEARCHING: 'View' near 31001 / notebook URL construction:")
idx = 0
count = 0
while count < 5:
    pos = js.find('"View"', idx)
    if pos == -1:
        break
    ctx = js[max(0, pos-300):pos+300]
    if "31001" in ctx or "notebook" in ctx.lower() or "job" in ctx.lower():
        count += 1
        print(f"\n  [{count}] pos={pos}: ...{ctx}...")
    idx = pos + 6

# ── 6. Look for fetch/axios calls that could return spark_job_id ─────────────
print("\n" + "=" * 70)
print("SEARCHING: API fetch calls with execution/run paths:")
for term in ["/v1/workflows/", "/v1/executions/", "/v1/runs/", "/v1/schedules/", "/element-runs/", "/job-runs/"]:
    pos = js.find(term)
    if pos != -1:
        ctx = js[max(0, pos-200):pos+300]
        print(f"\n  ✅ '{term}' at pos {pos}: ...{ctx}...")
    else:
        print(f"  ✗ '{term}' not found")

print("\nDone.")
