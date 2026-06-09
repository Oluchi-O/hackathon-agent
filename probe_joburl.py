"""
probe_joburl.py — Find where jobUrl comes from in the Element ML frontend.

Fetches main.7c45117e.js and searches for jobUrl, scheduleMetaId,
and the API endpoint that provides the notebook URL.
"""
import requests, re, os, urllib3
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

print("Fetching main.7c45117e.js (5.4MB)...")
r = requests.get(f"{MAIN}/element/static/js/main.7c45117e.js",
                 headers=headers, cookies=cookies, verify=False, timeout=60)
js = r.text
print(f"Downloaded: {len(js)} chars\n")

def show_all(term, context=400):
    """Print all occurrences of term with surrounding context."""
    idx = 0
    count = 0
    while True:
        pos = js.find(term, idx)
        if pos == -1:
            break
        count += 1
        ctx = js[max(0, pos-context):pos+context]
        print(f"\n  [{count}] '{term}' at pos {pos}:")
        print(f"  ...{ctx}...")
        idx = pos + len(term)
        if count >= 10:  # cap at 10 occurrences
            break
    if count == 0:
        print(f"  '{term}' not found")
    return count

print("=" * 70)
print("SEARCHING: jobUrl")
show_all("jobUrl", context=500)

print("\n" + "=" * 70)
print("SEARCHING: /v1/jobs/ (port 31001 path pattern)")
show_all("/v1/jobs/", context=300)

print("\n" + "=" * 70)
print("SEARCHING: scheduleMetaId (how frontend uses it)")
show_all("scheduleMetaId", context=300)

print("\n" + "=" * 70)
print("SEARCHING: workflowRunId")
show_all("workflowRunId", context=300)

print("\n" + "=" * 70)
print("SEARCHING: ELEMENT_WORKFLOW")
show_all("ELEMENT_WORKFLOW", context=300)

print("\n" + "=" * 70)
print("SEARCHING: 27801126 (known spark_job_id — should appear in notebook URL pattern)")
show_all("27801126", context=300)

print("\nDone.")
