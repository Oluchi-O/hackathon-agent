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
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from agent import diagnose_failure
from chat_agent import chat_turn
from data_layer import ElementMLClient

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
    """
    try:
        reply = chat_turn(
            message=request.message,
            history=request.history,
            bearer_token=request.bearer_token,
            cookie_string=request.cookie_string,
            associate_id=request.associate_id,
            notebook_json=request.notebook_json,
        )
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


# ── Dev server ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
