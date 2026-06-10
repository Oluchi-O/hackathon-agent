"""
probe_notebook_auth.py
Two goals:
1. Find fetchNotebookResource (Vs) and fetchWorkflowToken (md) implementations in JS
2. Probe :31001/v1/notebooks/88538 with alternative auth headers

Known:
  - :31001/v1/notebooks/88538 → 401 (endpoint EXISTS, standard Bearer fails)
  - fetchNotebookResource:()=>Vs  (in workflows module exports)
  - fetchWorkflowToken:()=>md      (in workflows module exports)
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

headers_bearer = {"Authorization": f"Bearer {BEARER}", "Accept": "application/json"}
headers_html   = {"Authorization": f"Bearer {BEARER}", "Accept": "text/html,*/*"}
MAIN      = "https://ml.prod.walmart.com"
API_BASE  = "https://ml.prod.walmart.com:31200"
JOB_BASE  = "https://ml.prod.walmart.com:31001"
PROJECT_ID  = "16701"
WORKFLOW_ID = "48227"
NOTEBOOK_ID = "88538"
SCHEDULE_ID = "895958"

# ── PART 1: Search JS for fetchNotebookResource & fetchWorkflowToken impls ───
print("Fetching app HTML to discover JS bundles...")
r = requests.get(f"{MAIN}/element/", headers=headers_html, cookies=cookies,
                 verify=False, timeout=15, allow_redirects=True)
html = r.text
scripts = re.findall(r'src=["\']([^"\']+\.js[^"\']*)["\']', html)
chunks  = re.findall(r'["\']([^"\']+\.[a-f0-9]{8}\.chunk\.js)["\']', html)
all_scripts = list(dict.fromkeys(scripts + chunks))
print(f"Found {len(all_scripts)} JS files")

workflow_token = None
notebook_resource_url = None

for s in all_scripts:
    url = s if s.startswith("http") else f"{MAIN}/{s.lstrip('/')}"
    try:
        jr = requests.get(url, headers=headers_html, cookies=cookies,
                          verify=False, timeout=30)
        content = jr.text
        if "fetchNotebookResource" not in content and "fetchWorkflowToken" not in content:
            continue
        print(f"\n{'='*70}")
        print(f"Bundle: {url} ({len(content)} chars)")
        print(f"{'='*70}")

        for term in ["fetchNotebookResource", "fetchWorkflowToken",
                     "notebookResource", "workflowToken", "notebookToken",
                     "notebookUrl", "notebooks/"]:
            start = 0
            count = 0
            while True:
                idx = content.find(term, start)
                if idx == -1:
                    break
                ctx = content[max(0, idx-300):idx+400]
                count += 1
                if count <= 3:
                    print(f"\n  [{count}] '{term}' at pos {idx}:")
                    print(f"  ...{ctx}...")
                start = idx + 1
    except Exception as e:
        print(f"  ERR {url}: {e}")

# ── PART 2: First try to get a workflow token ────────────────────────────────
print("\n" + "="*70)
print("PART 2: Fetch workflow token")
print("="*70)

# Try the token endpoint patterns common in this codebase
for path in [
    f"/v1/workflows/{WORKFLOW_ID}/token?projectId={PROJECT_ID}",
    f"/v1/workflows/token?workflowId={WORKFLOW_ID}&projectId={PROJECT_ID}",
    f"/v1/workflows/schedules/{SCHEDULE_ID}/token?projectId={PROJECT_ID}",
    f"/v1/notebooks/{NOTEBOOK_ID}/token?projectId={PROJECT_ID}",
    f"/v1/projects/{PROJECT_ID}/token",
]:
    try:
        rr = requests.get(f"{API_BASE}{path}", headers=headers_bearer, cookies=cookies,
                          verify=False, timeout=15)
        icon = "✅" if rr.status_code == 200 else "❌"
        print(f"\n  {icon} [{rr.status_code}] {path}")
        print(f"  Body[:300]: {rr.text[:300]}")
        if rr.status_code == 200:
            try:
                data = rr.json()
                workflow_token = data.get("token") or data.get("accessToken")
                print(f"  >> token: {workflow_token[:80] if workflow_token else 'not found in response'}")
            except Exception:
                pass
    except Exception as e:
        print(f"\n  [ERR] {path}: {e}")

# ── PART 3: Probe :31001/v1/notebooks/88538 with auth variants ───────────────
print("\n" + "="*70)
print("PART 3: :31001/v1/notebooks/88538 auth variants")
print("="*70)

paths_to_try = [
    f"/v1/notebooks/{NOTEBOOK_ID}",
    f"/v1/notebooks/{NOTEBOOK_ID}?projectId={PROJECT_ID}",
    f"/v1/notebooks/{NOTEBOOK_ID}/runs",
    f"/v1/notebooks/{NOTEBOOK_ID}/runs?projectId={PROJECT_ID}",
    f"/v1/notebooks/{NOTEBOOK_ID}/jobs",
    f"/v1/notebooks/{NOTEBOOK_ID}/jobs?projectId={PROJECT_ID}",
]

header_variants = [
    ("Bearer (standard)", {"Authorization": f"Bearer {BEARER}", "Accept": "application/json"}),
    ("No Auth header",    {"Accept": "application/json"}),
]
# Add workflow token if we got one
if workflow_token:
    header_variants.append(("Workflow token", {"Authorization": f"Bearer {workflow_token}", "Accept": "application/json"}))

# Also try with notebook_id used as a different type of token reference
for wt_header_name in ["X-Auth-Token", "X-Notebook-Token", "X-Jwt-Token"]:
    header_variants.append((f"{wt_header_name}", {wt_header_name: BEARER, "Accept": "application/json"}))

for path in paths_to_try:
    print(f"\n  PATH: {path}")
    for auth_name, hdrs in header_variants:
        try:
            rr = requests.get(f"{JOB_BASE}{path}", headers=hdrs, cookies=cookies,
                              verify=False, timeout=15)
            icon = "✅" if rr.status_code == 200 else ("⚠️ " if rr.status_code in (403, 404) else "❌")
            print(f"    {icon} [{rr.status_code}] {auth_name}: {rr.text[:200]}")
        except Exception as e:
            print(f"    [ERR] {auth_name}: {e}")

# ── PART 4: :31001 discovery — what other /v1/ paths exist ──────────────────
print("\n" + "="*70)
print("PART 4: :31001 endpoint discovery")
print("="*70)
for path in [
    "/v1/",
    "/v1/notebooks",
    f"/v1/notebooks?projectId={PROJECT_ID}",
    f"/v1/notebooks?notebookId={NOTEBOOK_ID}&projectId={PROJECT_ID}",
    f"/v1/projects/{PROJECT_ID}/notebooks",
    f"/v1/projects/{PROJECT_ID}/jobs",
    f"/v1/notebooks/{NOTEBOOK_ID}/history",
    f"/v1/notebooks/{NOTEBOOK_ID}/runs?limit=100",
]:
    try:
        rr = requests.get(f"{JOB_BASE}{path}", headers=headers_bearer, cookies=cookies,
                          verify=False, timeout=15)
        icon = "✅" if rr.status_code == 200 else "❌"
        print(f"\n  {icon} [{rr.status_code}] {path}")
        print(f"  Body[:300]: {rr.text[:300]}")
    except Exception as e:
        print(f"\n  [ERR] {path}: {e}")

print("\nDone.")
