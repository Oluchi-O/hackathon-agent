"""
failure_poller.py — Proactive failure detector for Element ML.

Polls /v1/workflows/schedules for configured projects and auto-diagnoses
any new FAILED run, saving results to alerts.json (served at /alerts).

Credentials: probe_creds.txt (already .gitignored)
  Line 1: Bearer token
  Line 2: Raw cookie string (from DevTools → Network → any request → Cookie header)

Config: poller_config.json
  {
    "project_ids": [16701],
    "associate_id": "o0o01od",
    "poll_interval_seconds": 120
  }

Run: python failure_poller.py
"""
import json
import os
import re
import sys
import time
from datetime import datetime, timezone

import urllib3
urllib3.disable_warnings()

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from agent import call_elementai
from chat_agent import _deep_diagnose_run_into_lines
from data_layer import ElementMLClient

CREDS_FILE  = os.path.join(SCRIPT_DIR, "probe_creds.txt")
CONFIG_FILE = os.path.join(SCRIPT_DIR, "poller_config.json")
ALERTS_FILE = os.path.join(SCRIPT_DIR, "alerts.json")
SEEN_FILE   = os.path.join(SCRIPT_DIR, ".poller_seen.json")

DEFAULT_POLL_INTERVAL = 120  # seconds


# ── I/O helpers ───────────────────────────────────────────────────────────────

def load_creds() -> tuple[str, dict]:
    """Return (bearer_token, cookies_dict) from probe_creds.txt."""
    if not os.path.exists(CREDS_FILE):
        print(f"[ERROR] {CREDS_FILE} not found.")
        print("Create it with:")
        print("  Line 1: your Bearer token (from DevTools → Authorization header)")
        print("  Line 2: your raw Cookie string (from DevTools → Cookie header)")
        sys.exit(1)
    with open(CREDS_FILE) as f:
        lines = f.read().splitlines()
    bearer = lines[0].replace("Bearer ", "").strip()
    cookie_str = lines[1].strip() if len(lines) > 1 else ""
    cookies: dict = {}
    for part in cookie_str.split(";"):
        if "=" in part:
            k, v = part.strip().split("=", 1)
            cookies[k.strip()] = v.strip()
    return bearer, cookies


def load_config() -> dict:
    if not os.path.exists(CONFIG_FILE):
        print(f"[ERROR] {CONFIG_FILE} not found.")
        print('Create it with: {"project_ids": [16701], "associate_id": "o0o01od", "poll_interval_seconds": 120}')
        sys.exit(1)
    with open(CONFIG_FILE) as f:
        return json.load(f)


def load_seen() -> set:
    if os.path.exists(SEEN_FILE):
        with open(SEEN_FILE) as f:
            return set(json.load(f))
    return set()


def save_seen(seen: set) -> None:
    with open(SEEN_FILE, "w") as f:
        json.dump(list(seen), f)


def load_alerts() -> list:
    if os.path.exists(ALERTS_FILE):
        with open(ALERTS_FILE) as f:
            return json.load(f)
    return []


def save_alerts(alerts: list) -> None:
    with open(ALERTS_FILE, "w") as f:
        json.dump(alerts, f, indent=2, default=str)


# ── Diagnosis ─────────────────────────────────────────────────────────────────

_DIAG_PROMPT = """You are an AI assistant for the Walmart Element ML Platform.
A PySpark job failure was automatically detected. Be concise and specific.

{job_data}

Respond with exactly two sections:
ROOT CAUSE: <one sentence stating what caused the failure>
FIX: <exact code change or action to resolve it — be specific, no "investigate further">"""


def diagnose_run(run: dict, project_id: int, client: ElementMLClient,
                 bearer: str, associate_id: str) -> dict:
    """Return an alert dict for one newly-detected failed run."""
    wf      = run.get("workflow", {})
    wf_name = wf.get("workflowName", "N/A") if isinstance(wf, dict) else "N/A"
    sid     = run.get("scheduleMetaId")

    # ── Step 1: extract structured error via existing deep-diagnose logic ─────
    lines: list[str] = []
    _deep_diagnose_run_into_lines(run, "FAILED RUN", project_id, client, lines)
    raw_diagnosis = "\n".join(lines)

    # Pull key fields from lines for structured storage
    error_type = cell_num = source_code = error_text = None
    for line in lines:
        m = re.search(r'\[Cell (\d+)\] ERROR — ([^:\n]+)', line)
        if m:
            cell_num, error_type = m.group(1), m.group(2).strip()
        if line.strip().startswith("Source code:"):
            source_code = line.split("Source code:", 1)[1].strip()
        if line.strip().startswith("Error message:"):
            error_text = line.split("Error message:", 1)[1].strip()

    # ── Step 2: LLM diagnosis (skip if no error section was extracted) ────────
    ai_diagnosis = None
    has_errors = "ERRORS FROM NOTEBOOK OUTPUT" in raw_diagnosis
    if has_errors:
        try:
            prompt = _DIAG_PROMPT.format(job_data=raw_diagnosis[:3000])
            ai_diagnosis = call_elementai(prompt, bearer, associate_id, max_tokens=2048)
        except Exception as e:
            ai_diagnosis = f"[LLM call failed: {e}]"
    else:
        ai_diagnosis = "[No error output extracted — notebook HTML may be unavailable for this run]"

    return {
        "scheduleMetaId": sid,
        "projectId": project_id,
        "workflow": wf_name,
        "failedAt": str(run.get("scheduleEndDate", ""))[:16],
        "detectedAt": datetime.now(timezone.utc).isoformat(),
        "errorType": error_type,
        "cellNum": cell_num,
        "sourceCode": source_code,
        "errorText": error_text,
        "aiDiagnosis": ai_diagnosis,
    }


# ── Polling ───────────────────────────────────────────────────────────────────

def seed_seen(project_ids: list, client: ElementMLClient, seen: set) -> None:
    """First-run: collect all existing failed IDs without diagnosing them."""
    total = 0
    for pid in project_ids:
        try:
            jobs = client.get_job_history(pid)
            for run in jobs:
                sid = run.get("scheduleMetaId")
                if sid is not None:
                    seen.add(sid)
                    total += 1
        except Exception as e:
            print(f"[SEED] project {pid} error: {e}")
    print(f"[POLLER] Seeded {len(seen)} scheduleMetaIds across {len(project_ids)} project(s). "
          "Only failures detected AFTER this moment will trigger alerts.")


def poll_once(project_ids: list, client: ElementMLClient, seen: set,
              alerts: list, bearer: str, associate_id: str) -> bool:
    """Single poll pass. Returns True if any new failures were found."""
    found_new = False

    for pid in project_ids:
        try:
            jobs = client.get_job_history(pid)
        except Exception as e:
            err = str(e)
            if "401" in err or "403" in err:
                print(f"[POLL] ⚠️  Auth failed for project {pid} — refresh probe_creds.txt with a new token")
            else:
                print(f"[POLL] project {pid} fetch error: {e}")
            continue

        failed = [j for j in jobs if j.get("scheduleStatus") == "FAILED"]

        for run in failed:
            sid = run.get("scheduleMetaId")
            if sid is None or sid in seen:
                continue

            print(f"[POLL] 🔴 New failure — project {pid}, scheduleMetaId={sid}")
            seen.add(sid)

            try:
                alert = diagnose_run(run, pid, client, bearer, associate_id)
            except Exception as e:
                alert = {
                    "scheduleMetaId": sid,
                    "projectId": pid,
                    "detectedAt": datetime.now(timezone.utc).isoformat(),
                    "workflow": "unknown",
                    "aiDiagnosis": f"[Diagnosis error: {e}]",
                }

            alerts.insert(0, alert)  # newest first
            found_new = True
            diag_preview = (alert.get("aiDiagnosis") or "")[:100]
            print(f"[POLL] ✅ Alert saved — {diag_preview}")

    return found_new


# ── User-initiated job tracker ───────────────────────────────────────────────

def track_job_until_terminal(
    schedule_meta_id: int,
    project_id: int,
    bearer: str,
    cookies: dict,
    associate_id: str,
    poll_interval: int = 60,
) -> None:
    """
    Poll until *schedule_meta_id* reaches a terminal state, then write an
    outcome entry to alerts.json.  Designed to run in a daemon thread.

    Terminal states: FAILED, KILLED, COMPLETED, SUCCESS, SUCCEEDED
    On 401/403: writes a TOKEN_EXPIRED alert so the user knows to refresh.
    """
    TERMINAL = {"FAILED", "KILLED", "COMPLETED", "SUCCESS", "SUCCEEDED"}
    MAX_POLLS = 60  # safety cap: ~60 min at default 60 s interval

    print(
        f"[TRACKER] 🟡 Tracking scheduleMetaId={schedule_meta_id} "
        f"in project {project_id}",
        flush=True,
    )
    client = ElementMLClient(bearer, cookies)
    poll_count = 0

    while True:
        poll_count += 1
        if poll_count > MAX_POLLS:
            timeout_alert = {
                "scheduleMetaId": schedule_meta_id,
                "projectId": project_id,
                "detectedAt": datetime.now(timezone.utc).isoformat(),
                "workflow": "unknown",
                "status": "TRACKER_TIMEOUT",
                "aiDiagnosis": (
                    f"[Tracker stopped after {MAX_POLLS} polls "
                    f"(~{MAX_POLLS * poll_interval // 60} min). "
                    "The job may still be running or completed with an unrecognised "
                    "status. Check the Element ML dashboard directly.]"
                ),
            }
            alerts = load_alerts()
            alerts.insert(0, timeout_alert)
            save_alerts(alerts)
            print(
                f"[TRACKER] ⏱️  Timeout after {MAX_POLLS} polls for "
                f"scheduleMetaId={schedule_meta_id}",
                flush=True,
            )
            return
        try:
            jobs = client.get_job_history(project_id)
        except Exception as e:
            err = str(e)
            if "401" in err or "403" in err:
                alert = {
                    "scheduleMetaId": schedule_meta_id,
                    "projectId": project_id,
                    "detectedAt": datetime.now(timezone.utc).isoformat(),
                    "workflow": "unknown",
                    "status": "TOKEN_EXPIRED",
                    "aiDiagnosis": (
                        "[Tracker stopped: Bearer token expired. "
                        "Open the agent, send any message to refresh credentials, "
                        f"then tell it to track project {project_id} again.]"
                    ),
                }
                alerts = load_alerts()
                alerts.insert(0, alert)
                save_alerts(alerts)
                print(
                    f"[TRACKER] ⚠️  Token expired — wrote alert for "
                    f"scheduleMetaId={schedule_meta_id}",
                    flush=True,
                )
                return
            print(
                f"[TRACKER] Fetch error (will retry in {poll_interval}s): {e}",
                flush=True,
            )
            time.sleep(poll_interval)
            continue

        run = next(
            (j for j in jobs if j.get("scheduleMetaId") == schedule_meta_id), None
        )
        if run is None:
            print(
                f"[TRACKER] scheduleMetaId={schedule_meta_id} not found — "
                f"will retry in {poll_interval}s",
                flush=True,
            )
            time.sleep(poll_interval)
            continue

        status = run.get("scheduleStatus", "")
        if status not in TERMINAL:
            print(
                f"[TRACKER] scheduleMetaId={schedule_meta_id} → {status} — "
                f"waiting {poll_interval}s…",
                flush=True,
            )
            time.sleep(poll_interval)
            continue

        # ── Terminal state reached ─────────────────────────────────────────
        print(
            f"[TRACKER] ✅ scheduleMetaId={schedule_meta_id} → {status}",
            flush=True,
        )

        if status in ("FAILED", "KILLED"):
            try:
                alert = diagnose_run(run, project_id, client, bearer, associate_id)
                alert["status"] = status
            except Exception as e:
                wf = run.get("workflow", {})
                wf_name = (
                    wf.get("workflowName", "unknown") if isinstance(wf, dict) else "unknown"
                )
                alert = {
                    "scheduleMetaId": schedule_meta_id,
                    "projectId": project_id,
                    "detectedAt": datetime.now(timezone.utc).isoformat(),
                    "workflow": wf_name,
                    "status": status,
                    "failedAt": str(run.get("scheduleEndDate", ""))[:16],
                    "aiDiagnosis": f"[Diagnosis error: {e}]",
                }
        else:
            wf = run.get("workflow", {})
            wf_name = wf.get("workflowName", "N/A") if isinstance(wf, dict) else "N/A"
            completed_at = str(run.get("scheduleEndDate", ""))[:16]
            alert = {
                "scheduleMetaId": schedule_meta_id,
                "projectId": project_id,
                "detectedAt": datetime.now(timezone.utc).isoformat(),
                "workflow": wf_name,
                "status": status,
                "completedAt": completed_at,
                "aiDiagnosis": f"✅ Job completed successfully at {completed_at}",
            }

        alerts = load_alerts()
        alerts.insert(0, alert)
        save_alerts(alerts)
        print(f"[TRACKER] 📝 Outcome written to {ALERTS_FILE}", flush=True)
        return


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    bearer, cookies = load_creds()
    config       = load_config()
    project_ids  = config["project_ids"]
    associate_id = config.get("associate_id", "")
    interval     = int(config.get("poll_interval_seconds", DEFAULT_POLL_INTERVAL))

    seen   = load_seen()
    alerts = load_alerts()

    client = ElementMLClient(bearer, cookies)

    print("=" * 60)
    print("Element ML Failure Poller")
    print(f"  Projects     : {project_ids}")
    print(f"  Poll interval: {interval}s")
    print(f"  Alerts file  : {ALERTS_FILE}")
    print(f"  Dashboard    : http://localhost:8000/alerts")
    print("=" * 60)

    # First run: seed seen state so we don't re-alert on existing failures
    if not seen:
        seed_seen(project_ids, client, seen)
        save_seen(seen)  # persist immediately before entering loop

    while True:
        now = datetime.now().strftime("%H:%M:%S")
        print(f"[POLL {now}] Checking {len(project_ids)} project(s)...")

        found = poll_once(project_ids, client, seen, alerts, bearer, associate_id)

        if found:
            save_alerts(alerts)
            save_seen(seen)

        time.sleep(interval)


if __name__ == "__main__":
    main()
