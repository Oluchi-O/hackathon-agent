"""
probe_notebook31999.py — Probe JupyterHub at :31999 for notebook source access.

Target: GET /api/contents/{filename} → returns .ipynb JSON
Known:
  - notebookId = 88538
  - mainScript = sample_cust_id_mapping.ipynb
  - Base path visible in UI: /home/jupyter/
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
NOTEBOOK_HOST = "https://ml.prod.walmart.com:31999"
PROJECT_ID = "16701"
NOTEBOOK_ID = "88538"
FILENAME = "sample_cust_id_mapping.ipynb"

def get(url, label=""):
    try:
        r = requests.get(url, headers=headers, cookies=cookies, verify=False, timeout=20)
        label = label or url
        print(f"\n  [{r.status_code}] {label}")
        if r.status_code == 200:
            print(f"  ✅ SUCCESS! body[:400]: {r.text[:400]}")
        else:
            print(f"  Body: {r.text[:400]}")
        return r
    except Exception as e:
        print(f"\n  [ERR] {label}: {e}")
        return None

print("=" * 70)
print("PROBE 1: Standard Jupyter Contents API")
print("=" * 70)

for path in [
    f"/api/contents/{FILENAME}",
    f"/api/contents/home/jupyter/{FILENAME}",
    f"/user/o0o01od/api/contents/{FILENAME}",
    f"/user/o0o01od/api/contents/home/jupyter/{FILENAME}",
]:
    get(f"{NOTEBOOK_HOST}{path}", path)

print("\n" + "=" * 70)
print("PROBE 2: Element ML wrapper paths")
print("=" * 70)

for path in [
    f"/v1/notebooks/{NOTEBOOK_ID}",
    f"/v1/notebooks/{NOTEBOOK_ID}/content",
    f"/v1/notebooks/{NOTEBOOK_ID}/source",
    f"/element/api/contents/{FILENAME}",
    f"/api/v1/notebooks/{NOTEBOOK_ID}",
    f"/api/v1/notebooks/{NOTEBOOK_ID}?projectId={PROJECT_ID}",
]:
    get(f"{NOTEBOOK_HOST}{path}", path)

print("\n" + "=" * 70)
print("PROBE 3: Try root listing to discover path structure")
print("=" * 70)

for path in [
    "/api/contents/",
    "/api/contents",
    "/user/o0o01od/api/contents/",
    "/api/kernels",
    "/api/status",
    "/api/",
]:
    get(f"{NOTEBOOK_HOST}{path}", path)

print("\nDone.")
