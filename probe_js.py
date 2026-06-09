"""
probe_js.py — Search Element ML JS bundles for the notebook URL API pattern.

Fetches all JS chunks from the SPA and searches for how it constructs
the /v1/jobs/{sparkJobId}/batch-{batchId}-{sparkJobId}.html URL.
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

# ── Fetch the SPA shell HTML ──────────────────────────────────────────────────
print("Fetching main app HTML...")
r = requests.get(f"{MAIN}/element/", headers=headers, cookies=cookies,
                 verify=False, timeout=15, allow_redirects=True)
html = r.text
print(f"Status: {r.status_code} | size: {len(html)} chars")

# Extract ALL script src values
scripts = re.findall(r'src=["\']([^"\']+\.js[^"\']*)["\']', html)
# Also look for chunk references in the HTML
chunks = re.findall(r'["\']([^"\']+\.[a-f0-9]{8}\.chunk\.js)["\']', html)
all_scripts = list(dict.fromkeys(scripts + chunks))
print(f"Found {len(all_scripts)} JS files: {all_scripts}")

# ── Search every JS bundle ────────────────────────────────────────────────────
SEARCH_TERMS = [
    "ELEMENT_WORKFLOW_RUN_ID",
    "elementWorkflowRunId",
    "workflowRunId",
    "sparkJobId",
    "batch-",          # appears in the HTML filename pattern
    "/v1/jobs/",       # the port 31001 path
    "batchId",
    "notebookUrl",
    "applicationUrl",
    "jobUrl",
]

found_anything = False
for s in all_scripts:
    url = s if s.startswith("http") else f"{MAIN}/{s.lstrip('/')}"
    try:
        jr = requests.get(url, headers=headers, cookies=cookies,
                          verify=False, timeout=30)
        content = jr.text
        print(f"\n--- {url} ({len(content)} chars, status {jr.status_code}) ---")
        for term in SEARCH_TERMS:
            idx = content.find(term)
            if idx != -1:
                # Show 300 chars around match
                ctx = content[max(0, idx-150):idx+200]
                print(f"  ✅ Found '{term}' at pos {idx}:")
                print(f"     ...{ctx}...")
                found_anything = True
    except Exception as e:
        print(f"  ERR {url}: {e}")

if not found_anything:
    print("\n\nNot found in JS bundles. Trying app page routes...")
    # Try specific routes that might SSR data including sparkJobIds
    for path in [
        f"/element/projects/16701",
        f"/element/projects/16701/applications",
        f"/element/projects/16701/workflows/48227",
        f"/element/projects/16701/workflows/48227/runs",
    ]:
        url = f"{MAIN}{path}"
        try:
            r2 = requests.get(url, headers=headers, cookies=cookies,
                              verify=False, timeout=15)
            # Look for embedded state or data containing the spark_job_id
            for term in ["27801126", "ELEMENT_WORKFLOW", "sparkJobId", "workflowRunId"]:
                if term in r2.text:
                    idx = r2.text.find(term)
                    ctx = r2.text[max(0, idx-100):idx+200]
                    print(f"  ✅ Found '{term}' in {path}:")
                    print(f"     ...{ctx}...")
                    found_anything = True
        except Exception as e:
            print(f"  ERR {path}: {e}")

print("\n\nDone.")
