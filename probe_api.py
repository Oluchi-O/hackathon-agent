"""
probe_api.py — One-time API endpoint discovery probe.

Run this ONCE with your Element ML credentials to find which endpoint
exposes ELEMENT_WORKFLOW_RUN_ID (the spark_job_id).

Usage:
    python probe_api.py

Paste your bearer token and cookies when prompted.
"""
import requests
import json
import sys
import urllib3

urllib3.disable_warnings()

API_BASE = "https://ml.prod.walmart.com:31200"
JOB_BASE = "https://ml.prod.walmart.com:31001"
MAIN_APP = "https://ml.prod.walmart.com"

# ── Load credentials from file ────────────────────────────────────────────────
import os
cred_file = os.path.join(os.path.dirname(__file__), "probe_creds.txt")
if os.path.exists(cred_file):
    with open(cred_file) as f:
        lines = f.read().splitlines()
    BEARER = lines[0].replace("Bearer ", "").strip()
    COOKIES_RAW = lines[1].strip() if len(lines) > 1 else ""
    print(f"✅ Loaded credentials from probe_creds.txt")
else:
    print(f"❌ Create {cred_file} with:")
    print("   Line 1: your bearer token (without 'Bearer ')")
    print("   Line 2: your full cookie string")
    sys.exit(1)

cookies = {}
for part in COOKIES_RAW.split(";"):
    if "=" in part:
        k, v = part.strip().split("=", 1)
        cookies[k.strip()] = v.strip()

headers = {
    "Authorization": f"Bearer {BEARER}",
    "Accept": "application/json",
}

SCHEDULE_META_ID = 895958
WORKFLOW_DAG_ID = "dag_48227_fe957158-dff8-43c0-ae7c-813cb13f5dab"

def probe(label, url, extra_headers=None, accept="application/json"):
    h = {**headers, "Accept": accept}
    if extra_headers:
        h.update(extra_headers)
    try:
        r = requests.get(url, headers=h, cookies=cookies, verify=False, timeout=10)
        body = r.text[:600]
        print(f"\n{'='*60}")
        print(f"[{r.status_code}] {label}")
        print(f"URL: {url}")
        if r.status_code == 200:
            print(f"✅ BODY: {body}")
        else:
            print(f"   body: {body[:200]}")
    except Exception as e:
        print(f"\n[ERR] {label}: {e}")

print("\n\n🔍 PROBING ELEMENT ML API ENDPOINTS...\n")

# ── Spring Actuator (reveals all endpoints) ───────────────────────────────────
probe("Spring Actuator root",          f"{API_BASE}/actuator")
probe("Spring Actuator mappings",      f"{API_BASE}/actuator/mappings")
probe("Spring Actuator beans",         f"{API_BASE}/actuator/beans")
probe("Spring Actuator env",           f"{API_BASE}/actuator/env")

# ── Airflow health/version (confirm Airflow API is accessible) ────────────────
probe("Airflow health",                f"{API_BASE}/api/v1/health")
probe("Airflow version",               f"{API_BASE}/api/v1/version")
probe("Airflow DAG list",              f"{API_BASE}/api/v1/dags?limit=5")
probe("Airflow config",                f"{API_BASE}/api/v1/config")

# ── Untried port 31200 paths ──────────────────────────────────────────────────
probe("applications list",             f"{API_BASE}/v1/applications?projectId=16701")
probe("spark-jobs by schedule",        f"{API_BASE}/v1/spark-jobs?scheduleMetaId={SCHEDULE_META_ID}")
probe("job-details",                   f"{API_BASE}/v1/job-details/{SCHEDULE_META_ID}")
probe("schedule history",              f"{API_BASE}/v1/schedules/{SCHEDULE_META_ID}/history")
probe("workflow executions",           f"{API_BASE}/v1/workflows/48227/executions?projectId=16701")
probe("dag task instances v1",         f"{API_BASE}/v1/dags/{WORKFLOW_DAG_ID}/task-instances")
probe("project schedule detail",       f"{API_BASE}/v1/projects/16701/schedules/{SCHEDULE_META_ID}")

# ── Port 31001 alternate paths ────────────────────────────────────────────────
probe("31001 actuator",                f"{JOB_BASE}/actuator")
probe("31001 actuator mappings",       f"{JOB_BASE}/actuator/mappings")
probe("31001 swagger",                 f"{JOB_BASE}/v3/api-docs")
probe("31001 swagger-ui",              f"{JOB_BASE}/swagger-ui.html", accept="text/html")
probe("31001 jobs index",              f"{JOB_BASE}/", accept="text/html")

# ── Main app SPA — look for JS bundles containing ELEMENT_WORKFLOW_RUN_ID ─────
print("\n\n🔍 FETCHING MAIN APP HTML (looking for JS bundles)...")
try:
    r = requests.get(MAIN_APP, headers={**headers, "Accept": "text/html"},
                     cookies=cookies, verify=False, timeout=15, allow_redirects=True)
    print(f"Main app: {r.status_code} | final URL: {r.url}")
    # Find script src references
    import re
    scripts = re.findall(r'src=["\']([^"\']+\.js[^"\']*)["\']', r.text)
    print(f"Found {len(scripts)} JS bundles: {scripts[:5]}")
    if scripts:
        # Search first few bundles for ELEMENT_WORKFLOW_RUN_ID
        for s in scripts[:3]:
            full_url = s if s.startswith("http") else f"{MAIN_APP}/{s.lstrip('/')}"
            try:
                jr = requests.get(full_url, headers=headers, cookies=cookies,
                                  verify=False, timeout=20)
                if "ELEMENT_WORKFLOW_RUN_ID" in jr.text or "elementWorkflowRunId" in jr.text:
                    # Find surrounding context
                    idx = jr.text.find("ELEMENT_WORKFLOW_RUN_ID")
                    if idx == -1:
                        idx = jr.text.find("elementWorkflowRunId")
                    print(f"\n✅ FOUND in {full_url}")
                    print(f"Context: ...{jr.text[max(0,idx-200):idx+300]}...")
                    break
            except Exception as e:
                print(f"JS bundle error {full_url}: {e}")
except Exception as e:
    print(f"Main app error: {e}")

print("\n\nDone. Look for any ✅ lines above.")
