"""
probe_rundetails.py — Find the API endpoint that returns runDetails / notebookUrl.

Since /v1/jobs/ is not in the JS, the URL must come from an API response.
Search for the endpoint that returns it.
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

print("Loading main.7c45117e.js...")
r = requests.get(f"{MAIN}/element/static/js/main.7c45117e.js",
                 headers=headers, cookies=cookies, verify=False, timeout=60)
js = r.text

def show_all(term, context=500, cap=8):
    idx = 0; count = 0
    while count < cap:
        pos = js.find(term, idx)
        if pos == -1: break
        count += 1
        ctx = js[max(0,pos-context):pos+context]
        print(f"\n  [{count}] pos={pos}: ...{ctx}...")
        idx = pos + len(term)
    if count == 0:
        print(f"  NOT FOUND: {term}")
    return count

print("\n" + "="*70)
print("SEARCHING: runDetails (the real-time status object)")
show_all("runDetails", context=600)

print("\n" + "="*70)
print("SEARCHING: 31001 (port number of content server)")
show_all("31001", context=400)

print("\n" + "="*70)
print("SEARCHING: notebookUrl")
show_all("notebookUrl", context=400)

print("\n" + "="*70)
print("SEARCHING: applicationUrl")
show_all("applicationUrl", context=400)

print("\n" + "="*70)
print("SEARCHING: jobLink / job_link / jobLink")
show_all("jobLink", context=400)
show_all("job_link", context=400)

print("\n" + "="*70)
print("SEARCHING: viewUrl / view_url")
show_all("viewUrl", context=400)

print("\n" + "="*70)
print("SEARCHING: openNotebook / open_notebook / openApplication")
show_all("openNotebook", context=400)
show_all("openApplication", context=400)

print("\n" + "="*70)
print("SEARCHING: dagRunId / dag_run_id (Airflow run identifier)")
show_all("dagRunId", context=400)

print("\n" + "="*70)
print("SEARCHING: executionDate (how Airflow run is identified)")
show_all("executionDate", context=400, cap=5)

print("\n" + "="*70)
print("ALSO LOADING 372 chunk and searching...")
r2 = requests.get(f"{MAIN}/element/static/js/372.3a9b8fa9.js",
                  headers=headers, cookies=cookies, verify=False, timeout=60)
js2 = r2.text
print(f"372 chunk: {len(js2)} chars")
for term in ["31001", "notebookUrl", "runDetails", "jobLink", "viewUrl", "/v1/jobs/"]:
    idx = js2.find(term)
    if idx != -1:
        ctx = js2[max(0,idx-300):idx+300]
        print(f"\n  ✅ Found '{term}' in 372 chunk: ...{ctx}...")
    else:
        print(f"  '{term}' not in 372 chunk")

print("\nDone.")
