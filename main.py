"""
main.py — FastAPI service for the Element ML Compute Intelligence Agent.

Endpoints:
  GET  /health                        → liveness check
  GET  /jobs/{project_id}             → list failed runs for a project
  POST /diagnose                      → AI diagnosis for a failed run
  GET  /workflow/{workflow_id}/config → current spark config for a workflow

Run locally:
  uvicorn main:app --reload --port 8000

Auth:
  All endpoints require  Authorization: Bearer <token>  header.
  Cookie string optional — required for some platform endpoints.
"""
import json
import os
import threading
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from agent import diagnose_failure
from chat_agent import chat_turn, _extract_ids, _is_track_intent
from data_layer import ElementMLClient
from failure_poller import track_job_until_terminal

# ── Job-tracking dedup (prevents duplicate threads for the same scheduleMetaId)
_TRACKING: set[int] = set()
_tracking_lock = threading.Lock()


def _run_tracker(
    schedule_meta_id: int,
    project_id: int,
    bearer: str,
    cookie_string: str,
    associate_id: str,
) -> None:
    """Thread target: track one job until terminal, then remove from _TRACKING."""
    cookies: dict = {}
    for part in cookie_string.split(";"):
        if "=" in part:
            k, v = part.strip().split("=", 1)
            cookies[k.strip()] = v.strip()
    try:
        track_job_until_terminal(schedule_meta_id, project_id, bearer, cookies, associate_id)
    except Exception as e:
        print(f"[TRACKER] Unexpected error for sid={schedule_meta_id}: {e}", flush=True)
    finally:
        with _tracking_lock:
            _TRACKING.discard(schedule_meta_id)

app = FastAPI(
    title="Element ML Compute Intelligence Agent",
    description=(
        "AI-powered failure diagnosis and compute optimization for the "
        "Walmart Element ML Platform. Hackathon: Everybody Hacks 2026, Track T04-02."
    ),
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── HTML UI ───────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def root():
    """Serve the chat UI."""
    with open("static/index.html", encoding="utf-8") as f:
        return f.read()


# ── Request / response models ─────────────────────────────────────────────────

class DiagnoseRequest(BaseModel):
    schedule_meta_id: int
    project_id: int
    workflow_meta_id: int
    spark_job_id: int
    cookies: dict = {}

    model_config = {"json_schema_extra": {
        "example": {
            "schedule_meta_id": 895958,
            "project_id": 16701,
            "workflow_meta_id": 48227,
            "spark_job_id": 27801126,
            "cookies": {}
        }
    }}


class DiagnoseResponse(BaseModel):
    scheduleMetaId: int
    status: str
    workflowName: Optional[str] = None
    mainScript: Optional[str] = None
    errorsFound: list = []
    warnings: list = []
    sparkConfig: dict = {}
    clusterUptime: Optional[str] = None
    diagnosis: str
    jobHistorySummary: dict = {}


# ── Helper: parse bearer token ────────────────────────────────────────────────

def _token(authorization: str) -> str:
    return authorization.removeprefix("Bearer ").strip()


def _cookies_from_header(cookie_header: str) -> dict:
    cookies = {}
    for part in cookie_header.split(";"):
        if "=" in part:
            k, v = part.strip().split("=", 1)
            cookies[k.strip()] = v.strip()
    return cookies


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health", tags=["Infra"])
def health():
    """Liveness check."""
    return {"status": "ok", "service": "Element ML Compute Intelligence Agent"}


@app.get("/jobs/{project_id}", tags=["Data"])
def list_failed_jobs(
    project_id: int,
    authorization: str = Header(..., description="Bearer token from SSO"),
    cookie: str = Header(default="", description="Full cookie string from DevTools"),
):
    """
    List all FAILED runs for a project (KILLED runs excluded).
    Returns schedule IDs, timestamps, and workflow metadata — use these
    to identify which run to diagnose.
    """
    client = ElementMLClient(_token(authorization), _cookies_from_header(cookie))
    try:
        history = client.get_job_history(project_id)
        failed = [
            {
                "scheduleMetaId": r.get("scheduleMetaId"),
                "workflowMetaId": r.get("workflowMetaId"),
                "workflowDagId": r.get("workflowDagId"),
                "workflowName": (
                    r.get("workflow", {}).get("workflowName")
                    if isinstance(r.get("workflow"), dict)
                    else None
                ),
                "startDate": r.get("scheduleStartDate"),
                "endDate": r.get("scheduleEndDate"),
                "status": r.get("scheduleStatus"),
            }
            for r in history
            if r.get("scheduleStatus") == "FAILED"
        ]
        return {
            "projectId": project_id,
            "failedRuns": failed,
            "count": len(failed),
            "totalRuns": len(history),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/diagnose", response_model=DiagnoseResponse, tags=["Agent"])
def diagnose(
    request: DiagnoseRequest,
    authorization: str = Header(..., description="Bearer token from SSO"),
    cookie: str = Header(default="", description="Full cookie string from DevTools"),
):
    """
    Diagnose a failed job run.

    Provide the IDs from the /jobs/{project_id} response plus the spark_job_id
    (visible in the Element ML cost dashboard or node run details).

    Returns an AI-generated diagnosis naming the specific root cause and exact fix.
    """
    cookies = {**_cookies_from_header(cookie), **request.cookies}

    try:
        result = diagnose_failure(
            schedule_meta_id=request.schedule_meta_id,
            project_id=request.project_id,
            workflow_meta_id=request.workflow_meta_id,
            spark_job_id=request.spark_job_id,
            bearer_token=_token(authorization),
            cookies=cookies,
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Diagnosis failed: {str(e)}")


@app.get("/workflow/{workflow_id}/config", tags=["Data"])
def get_workflow_config(
    workflow_id: int,
    project_id: int = Query(..., description="Element ML project ID"),
    authorization: str = Header(..., description="Bearer token from SSO"),
    cookie: str = Header(default="", description="Full cookie string from DevTools"),
):
    """
    Get the current spark configuration for a workflow.
    Useful for reviewing what a job is configured to use before submitting.
    """
    client = ElementMLClient(_token(authorization), _cookies_from_header(cookie))
    try:
        return client.get_workflow_definition(workflow_id, project_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Chat endpoint ─────────────────────────────────────────────────────────────

class ChatRequest(BaseModel):
    message: str
    history: list = []
    bearer_token: str
    cookie_string: str = ""
    associate_id: str = ""
    notebook_json: Optional[dict] = None  # stripped .ipynb cells from file attachment


@app.post("/chat", tags=["Agent"])
def chat(request: ChatRequest):
    """
    Conversational endpoint for the chat UI.

    Accepts a plain-English message + conversation history.
    Automatically fetches live job data when a project_id is mentioned,
    then returns an AI-generated response.

    When the user says they just submitted a job, a background tracker thread
    polls until the job reaches a terminal state and writes the outcome
    (success confirmation or full failure diagnosis) to alerts.json.
    """
    try:
        track = _is_track_intent(request.message, request.history)

        reply = chat_turn(
            message=request.message,
            history=request.history,
            bearer_token=request.bearer_token,
            cookie_string=request.cookie_string,
            associate_id=request.associate_id,
            notebook_json=request.notebook_json,
            track_intent=track,
        )

        # User-initiated job tracking
        if track and request.bearer_token:
            full_text = " ".join(
                m.get("content", "") for m in request.history
            ) + " " + request.message
            ids = _extract_ids(full_text)
            project_id = ids.get("project_id")

            if project_id:
                _TERMINAL = {"FAILED", "KILLED", "COMPLETED", "SUCCESS", "SUCCEEDED"}
                cookies = _cookies_from_header(request.cookie_string)
                client = ElementMLClient(request.bearer_token, cookies)
                try:
                    jobs = client.get_job_history(project_id)
                    active = [j for j in jobs if j.get("scheduleStatus") not in _TERMINAL]
                    if active:
                        sid = active[0].get("scheduleMetaId")
                        if sid:
                            with _tracking_lock:
                                already = sid in _TRACKING
                                if not already:
                                    _TRACKING.add(sid)
                            if not already:
                                threading.Thread(
                                    target=_run_tracker,
                                    args=(
                                        sid,
                                        project_id,
                                        request.bearer_token,
                                        request.cookie_string,
                                        request.associate_id,
                                    ),
                                    daemon=True,
                                ).start()
                                print(
                                    f"[CHAT] 🟡 Tracker started — "
                                    f"scheduleMetaId={sid}, project={project_id}",
                                    flush=True,
                                )
                            else:
                                print(
                                    f"[CHAT] Already tracking scheduleMetaId={sid}",
                                    flush=True,
                                )
                except Exception as e:
                    print(f"[CHAT] Could not start tracker: {e}", flush=True)

        return {"reply": reply}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Chat failed: {str(e)}")


# ── Probe: full Airflow task log ─────────────────────────────────────────────
# TODO: remove — Papermill doesn't log cell source; file attachment is the correct solution.

@app.get("/probe/airflow-log", tags=["Probe"])
def probe_airflow_log(
    workflow_dag_id: str = Query(..., description="workflowDagId from the job history response"),
    run_index: int = Query(0, description="0=most recent run, 1=second most recent, etc."),
    max_chars: int = Query(80000, description="Max log chars to return (default 80k)"),
    authorization: str = Header(..., description="Bearer token from SSO"),
    cookie: str = Header(default="", description="Full cookie string from DevTools"),
):
    """
    Fetch the full Airflow task log for a workflow's most recent run.

    Use this to check whether Papermill logs notebook cell source code,
    which would allow pre-run code review via the working auth chain.

    Inspect `has_cells_keyword` first — if True, check `log_text` for cell blocks.
    If False, cell source is NOT present in Airflow logs and another approach is needed.
    """
    client = ElementMLClient(_token(authorization), _cookies_from_header(cookie))
    try:
        return client.get_airflow_task_log_full(workflow_dag_id, run_index, max_chars)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Proactive failure alerts dashboard ───────────────────────────────────────

@app.get("/alerts", tags=["Poller"], response_class=HTMLResponse, include_in_schema=False)
def alerts_dashboard():
    """Live dashboard of auto-detected failures from failure_poller.py."""
    alerts_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "alerts.json")
    alerts = []
    if os.path.exists(alerts_path):
        with open(alerts_path) as f:
            alerts = json.load(f)

    rows = ""
    for a in alerts:
        diag = (a.get("aiDiagnosis") or "—").replace("<", "&lt;").replace(">", "&gt;")
        src  = (a.get("sourceCode") or "—").replace("<", "&lt;").replace(">", "&gt;")
        status_val = a.get('status') or ('FAILED' if a.get('failedAt') else '—')
        status_color = '#4ade80' if status_val in ('COMPLETED','SUCCESS','SUCCEEDED') else ('#ef4444' if status_val in ('FAILED','KILLED') else '#f59e0b')
        rows += f"""
        <tr>
          <td>{a.get('detectedAt', '')[:19].replace('T', ' ')}</td>
          <td>{a.get('projectId', '—')}</td>
          <td>{a.get('workflow', '—')}</td>
          <td>{a.get('scheduleMetaId', '—')}</td>
          <td><span style="color:{status_color};font-weight:600">{status_val}</span></td>
          <td>{a.get('errorType', '—')}</td>
          <td>Cell {a.get('cellNum', '—')}</td>
          <td><code>{src[:120]}{'…' if len(src) > 120 else ''}</code></td>
          <td class="diag">{diag}</td>
        </tr>"""

    count = len(alerts)
    latest_ts = alerts[0].get('detectedAt', '') if alerts else ''
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Element ML — Failure Alerts</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
           background: #0d1117; color: #e6edf3; margin: 0; padding: 24px; }}
    h1   {{ color: #f0883e; margin-bottom: 4px; }}
    .meta {{ color: #8b949e; font-size: 13px; margin-bottom: 20px; }}
    .badge {{ background: #da3633; color: #fff; padding: 2px 8px;
              border-radius: 12px; font-size: 12px; margin-left: 8px; }}
    .live  {{ color: #4ade80; font-size: 12px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th    {{ background: #161b22; color: #8b949e; text-align: left;
             padding: 8px 12px; border-bottom: 1px solid #30363d; }}
    td    {{ padding: 8px 12px; border-bottom: 1px solid #21262d; vertical-align: top; }}
    tr:hover td {{ background: #161b22; }}
    code  {{ background: #161b22; padding: 2px 6px; border-radius: 4px;
             font-family: 'SF Mono', Consolas, monospace; font-size: 12px; }}
    .diag {{ max-width: 380px; white-space: pre-wrap; font-size: 12px; color: #cdd9e5; }}
    .empty {{ text-align: center; padding: 60px; color: #8b949e; }}
  </style>
</head>
<body>
  <h1>🔴 Element ML — Proactive Failure Alerts
    {'<span class="badge">' + str(count) + ' alert' + ('s' if count != 1 else '') + '</span>' if count else ''}
  </h1>
  <div class="meta">
    <span class="live" id="live-status">● Live</span> &nbsp;·&nbsp;
    <a href="/alerts/data" style="color:#58a6ff">Raw JSON</a> &nbsp;·&nbsp;
    Updates automatically when new alerts arrive
  </div>
  {'<table><thead><tr>'
    '<th>Detected At (UTC)</th><th>Project</th><th>Workflow</th>'
    '<th>scheduleMetaId</th><th>Status</th><th>Error Type</th>'
    '<th>Cell</th><th>Source</th><th>AI Diagnosis</th>'
    '</tr></thead><tbody>' + rows + '</tbody></table>'
    if count else '<div class="empty" id="empty-msg">✅ No alerts yet — tracker will update this page when a monitored job finishes.</div>'}
<script>
  var _knownCount = {count};
  var _knownTs    = '{latest_ts}';

  async function poll() {{
    try {{
      var r = await fetch('/alerts/data');
      if (!r.ok) return;
      var data = await r.json();
      var count = data.length;
      var ts    = count > 0 ? (data[0].detectedAt || '') : '';
      if (count !== _knownCount || ts !== _knownTs) {{
        location.reload();
      }}
    }} catch(e) {{}}
  }}

  setInterval(poll, 15000);
</script>
</body>
</html>"""
    return html


@app.get("/alerts/data", tags=["Poller"])
def alerts_data():
    """Raw JSON of all auto-detected failure alerts."""
    alerts_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "alerts.json")
    if not os.path.exists(alerts_path):
        return []
    with open(alerts_path) as f:
        return json.load(f)


# ── Dev server ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
