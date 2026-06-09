"""
probe_source_access.py — Three-angle probe for pre-run notebook source access.

Angle 1: :31001  — does it serve the raw .ipynb alongside .html?
Angle 2: :31999  — JupyterHub Contents API with cookies-only (no Bearer;
          JupyterHub uses session-cookie auth, not SSO Bearer tokens)
Angle 3: :31200  — any project-level file/notebook listing endpoints

Run: python3 probe_source_access.py
"""
import requests, json, os, re, sys
import urllib3
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

print(f"Loaded {len(cookies)} cookies. Keys: {list(cookies.keys())[:12]}", flush=True)

# Known values from previous successful probes
SPARK_JOB_ID = 27801126
BATCH_ID     = 88538
PROJECT_ID   = 16701
WORKFLOW_ID  = 48227
FILENAME     = "sample_cust_id_mapping.ipynb"

API_BASE  = "https://ml.prod.walmart.com:31200"
JOB_BASE  = "https://ml.prod.walmart.com:31001"
NB_BASE   = "https://ml.prod.walmart.com:31999"

bearer_headers = {
    "Authorization": f"Bearer {BEARER}",
    "Accept": "application/json",
}

# ─── helper ──────────────────────────────────────────────────────────────────
def get(url, label, headers, cookies=cookies, note=""):
    try:
        r = requests.get(url, headers=headers, cookies=cookies,
                         verify=False, timeout=20)
        ct = r.headers.get("content-type", "")
        body = r.text[:500]
        tag = "✅" if r.status_code == 200 and "text/html" not in ct else (
              "⚠️ HTML" if "text/html" in ct else "❌")
        print(f"\n{tag} [{r.status_code}] {label}", flush=True)
        if note:
            print(f"   note: {note}", flush=True)
        print(f"   Content-Type: {ct[:80]}", flush=True)
        print(f"   Body[:500]: {body}", flush=True)
        return r
    except Exception as e:
        print(f"\n[ERR] {label}: {e}", flush=True)
        return None

# ─── ANGLE 1: :31001 raw .ipynb ──────────────────────────────────────────────
print("\n" + "="*70, flush=True)
print("ANGLE 1: :31001 — raw .ipynb alongside executed .html", flush=True)
print("="*70, flush=True)

for ext in [".ipynb", ".json"]:
    path = f"/v1/jobs/{SPARK_JOB_ID}/batch-{BATCH_ID}-{SPARK_JOB_ID}{ext}"
    get(f"{JOB_BASE}{path}", path, {**bearer_headers, "Accept": "application/json"})

# Also try without spark job prefix — just the notebook id
for path in [
    f"/v1/notebooks/{BATCH_ID}",
    f"/v1/notebooks/{BATCH_ID}?projectId={PROJECT_ID}",
    f"/v1/projects/{PROJECT_ID}/notebooks/{BATCH_ID}",
    f"/v1/projects/{PROJECT_ID}/notebooks/{BATCH_ID}/source",
    f"/v1/projects/{PROJECT_ID}/notebooks/{BATCH_ID}/download",
]:
    get(f"{JOB_BASE}{path}", path, bearer_headers)

# ─── ANGLE 2: :31999 JupyterHub Contents API — COOKIES ONLY ─────────────────
print("\n" + "="*70, flush=True)
print("ANGLE 2: :31999 JupyterHub — cookies-only (no Bearer token)", flush=True)
print("="*70, flush=True)

# Extract XSRFToken from cookies if present
xsrf = cookies.get("_xsrf", cookies.get("_xsrftoken", ""))
print(f"\n_xsrf cookie found: {'YES — ' + xsrf[:30] if xsrf else 'NO'}", flush=True)

# JupyterHub typically uses session cookies; try with Accept: application/json
jh_headers_base = {"Accept": "application/json"}
jh_headers_xsrf = {**jh_headers_base, "X-XSRFToken": xsrf} if xsrf else jh_headers_base

# Check for any jupyter/hub related cookies
jupyter_cookies = {k: v for k, v in cookies.items()
                   if any(kw in k.lower() for kw in ["jupyter", "hub", "xsrf", "session"])}
print(f"Jupyter-related cookies: {list(jupyter_cookies.keys())}", flush=True)

# Try the single-user server paths
user = "o0o01od"
for path in [
    f"/user/{user}/api/contents",
    f"/user/{user}/api/contents/",
    f"/user/{user}/api/contents/{FILENAME}",
    f"/user/{user}/api/contents/home/jupyter/{FILENAME}",
    f"/api/contents",
    f"/api/contents/{FILENAME}",
    f"/hub/api/contents/{FILENAME}",
]:
    get(f"{NB_BASE}{path}", path, jh_headers_xsrf,
        note="cookies-only, no Bearer")

# ─── ANGLE 3: :31200 project-level file listing ──────────────────────────────
print("\n" + "="*70, flush=True)
print("ANGLE 3: :31200 — project-level file/notebook listing endpoints", flush=True)
print("="*70, flush=True)

for path in [
    f"/v1/projects/{PROJECT_ID}/files",
    f"/v1/projects/{PROJECT_ID}/files?type=notebook",
    f"/v1/projects/{PROJECT_ID}/notebooks",
    f"/v1/projects/{PROJECT_ID}/notebooks?filename={FILENAME}",
    f"/v1/notebooks?projectId={PROJECT_ID}&filename={FILENAME}",
    f"/v1/files?projectId={PROJECT_ID}",
    f"/v1/files?projectId={PROJECT_ID}&name={FILENAME}",
    f"/v1/notebooks/{BATCH_ID}/content?projectId={PROJECT_ID}",
    f"/v1/notebooks/{BATCH_ID}/download?projectId={PROJECT_ID}",
    f"/v1/projects/{PROJECT_ID}/notebooks/{BATCH_ID}/content",
    f"/v1/workflows/{WORKFLOW_ID}/notebookContent?projectId={PROJECT_ID}",
    f"/v1/workflows/{WORKFLOW_ID}/files?projectId={PROJECT_ID}",
]:
    get(f"{API_BASE}{path}", path, bearer_headers)

print("\n\nDone.", flush=True)
