"""
probe_source2.py — Follow-up probes based on probe_source_access.py results.

Key findings to investigate:
1. :31001 .ipynb returns 200 + EMPTY body — check headers, try streaming
2. :31999 base href="/element/" — try /element/user/{user}/api/contents/
3. :31200 500s — try with extra headers to get error detail

Run: python3 probe_source2.py
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

SPARK_JOB_ID = 27801126
BATCH_ID     = 88538
PROJECT_ID   = 16701
WORKFLOW_ID  = 48227
FILENAME     = "sample_cust_id_mapping.ipynb"
USER         = "o0o01od"

API_BASE = "https://ml.prod.walmart.com:31200"
JOB_BASE = "https://ml.prod.walmart.com:31001"
NB_BASE  = "https://ml.prod.walmart.com:31999"

bearer_headers = {
    "Authorization": f"Bearer {BEARER}",
    "Accept": "application/json",
}

def probe(url, label, headers, cookies=cookies, stream=False):
    try:
        r = requests.get(url, headers=headers, cookies=cookies,
                         verify=False, timeout=30, stream=stream)
        ct = r.headers.get("content-type", "—")
        cl = r.headers.get("content-length", "—")
        te = r.headers.get("transfer-encoding", "—")
        body_preview = r.text[:600] if not stream else "(streaming — see below)"
        tag = "✅" if r.status_code == 200 and "html" not in ct else (
              "⚠️ HTML" if "html" in ct else "❌")
        print(f"\n{tag} [{r.status_code}] {label}", flush=True)
        print(f"   Content-Type   : {ct}", flush=True)
        print(f"   Content-Length : {cl}", flush=True)
        print(f"   Transfer-Enc   : {te}", flush=True)
        print(f"   All headers    : {dict(r.headers)}", flush=True)
        if stream:
            chunks = []
            for chunk in r.iter_content(8192):
                chunks.append(chunk)
                if sum(len(c) for c in chunks) > 1000:
                    break
            body_preview = b"".join(chunks)[:600].decode("utf-8", errors="replace")
        print(f"   Body[:600]     : {body_preview}", flush=True)
        return r
    except Exception as e:
        print(f"\n[ERR] {label}: {e}", flush=True)
        return None

# ─── 1. Deep dive: .ipynb and .json empty response ───────────────────────────
print("\n" + "="*70, flush=True)
print("PART 1: Deep headers on .ipynb / .json from :31001", flush=True)
print("="*70, flush=True)

probe(
    f"{JOB_BASE}/v1/jobs/{SPARK_JOB_ID}/batch-{BATCH_ID}-{SPARK_JOB_ID}.ipynb",
    ".ipynb — stream mode",
    {**bearer_headers, "Accept": "*/*"},
    stream=True
)
probe(
    f"{JOB_BASE}/v1/jobs/{SPARK_JOB_ID}/batch-{BATCH_ID}-{SPARK_JOB_ID}.json",
    ".json — stream mode",
    {**bearer_headers, "Accept": "*/*"},
    stream=True
)

# Compare with .html to see its headers too
probe(
    f"{JOB_BASE}/v1/jobs/{SPARK_JOB_ID}/batch-{BATCH_ID}-{SPARK_JOB_ID}.html",
    ".html (known-good) — headers only",
    {**bearer_headers, "Accept": "text/html"},
    stream=True
)

# ─── 2. :31999 with /element/ prefix (base href="/element/") ─────────────────
print("\n" + "="*70, flush=True)
print("PART 2: :31999 with /element/ prefix from <base href>", flush=True)
print("="*70, flush=True)

jh_headers = {"Accept": "application/json"}  # cookies-only, no Bearer

for path in [
    f"/element/user/{USER}/api/contents",
    f"/element/user/{USER}/api/contents/",
    f"/element/user/{USER}/api/contents/{FILENAME}",
    f"/element/user/{USER}/api/contents/home/jupyter/{FILENAME}",
    f"/element/api/contents",
    f"/element/api/contents/{FILENAME}",
    f"/element/hub/api/contents/{FILENAME}",
    # Also try with Bearer since Element ML wraps JupyterHub
    f"/element/user/{USER}/api/contents/{FILENAME}",
]:
    probe(f"{NB_BASE}{path}", path, jh_headers)

# Same paths but WITH Bearer token
print("\n--- same paths WITH Bearer token ---", flush=True)
for path in [
    f"/element/user/{USER}/api/contents",
    f"/element/user/{USER}/api/contents/{FILENAME}",
    f"/element/api/contents/{FILENAME}",
]:
    probe(f"{NB_BASE}{path}", f"{path} [+Bearer]", bearer_headers)

# ─── 3. :31200 500s — try to extract error detail ────────────────────────────
print("\n" + "="*70, flush=True)
print("PART 3: :31200 500 endpoints — get error body", flush=True)
print("="*70, flush=True)

# These returned 500 before — show full body to understand what's missing
for path in [
    f"/v1/projects/{PROJECT_ID}/files",
    f"/v1/projects/{PROJECT_ID}/notebooks",
    f"/v1/notebooks/{BATCH_ID}/content?projectId={PROJECT_ID}",
]:
    probe(f"{API_BASE}{path}", path, bearer_headers)

# ─── 4. Try :31999 hub API for user token ────────────────────────────────────
print("\n" + "="*70, flush=True)
print("PART 4: :31999 /hub/api — JupyterHub admin endpoints", flush=True)
print("="*70, flush=True)

for path in [
    f"/hub/api",
    f"/hub/api/users/{USER}",
    f"/hub/api/users/{USER}/server",
    f"/element/hub/api",
    f"/element/hub/api/users/{USER}",
]:
    probe(f"{NB_BASE}{path}", path, bearer_headers)

print("\n\nDone.", flush=True)
