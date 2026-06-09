"""
agent.py — Failure diagnosis pipeline.

Flow:
  1. Collect data from Element ML APIs (job history, spark config, error logs)
  2. Parse rendered notebook HTML for actual error messages
  3. Assemble a structured RAG context
  4. Call ElementAI LLM for specific, actionable diagnosis
  5. Return structured response

ElementAI endpoint: set ELEMENTAI_URL + ELEMENTAI_MODEL in .env or environment.
"""
import os
import json
import time
import base64
import urllib3
import requests
from datetime import datetime
from dotenv import load_dotenv
from Crypto.PublicKey import RSA
from Crypto.Signature import PKCS1_v1_5
from Crypto.Hash import SHA256

load_dotenv()  # loads .env from the project directory

from data_layer import ElementMLClient
from html_parser import build_notebook_context

urllib3.disable_warnings()

# ── Walmart LLM Gateway config (set in .env) ─────────────────────────────────
CONSUMER_ID      = os.getenv("CONSUMER_ID", "")
KEY_VERSION      = int(os.getenv("KEY_VERSION", "1"))
PVT_KEY_PATH     = os.getenv("PVT_KEY_BASE64_PATH", "")
WMT_CA_PATH      = os.getenv("WMT_CA_PATH", "")
WMT_GATEWAY_URL      = os.getenv("WMT_GATEWAY_URL", "https://wmtllmgateway.stage.walmart.com/wmtllmgateway")
LLM_MODEL            = os.getenv("LLM_MODEL", "gemini-1.5-flash")
LLM_MODEL_VERSION    = os.getenv("LLM_MODEL_VERSION", "002")

# Set SSL cert for requests if CA path provided
if WMT_CA_PATH and os.path.exists(WMT_CA_PATH):
    os.environ["REQUESTS_CA_BUNDLE"] = WMT_CA_PATH
    os.environ["SSL_CERT_FILE"] = WMT_CA_PATH


def _get_gateway_headers(associate_id: str = "") -> dict:
    """
    Build RSA-signed headers required by the Walmart LLM Gateway.
    associate_id: the caller's Walmart associate ID (e.g. 'o0o01od').
                  Required by the gateway as wm_llm_gw.user_name.
    """
    if not CONSUMER_ID:
        raise ValueError("CONSUMER_ID is not set. Add it to your .env file.")
    if not PVT_KEY_PATH or not os.path.exists(PVT_KEY_PATH):
        raise ValueError(f"PVT_KEY_BASE64_PATH not set or file not found: {PVT_KEY_PATH}")
    if not associate_id:
        raise ValueError("Associate ID is required. Please enter it on the connect screen.")

    with open(PVT_KEY_PATH, "r") as f:
        pvt_key_b64 = f.read().strip()

    rsa_pem = base64.b64decode(pvt_key_b64)
    timestamp = int(time.time()) * 1000
    data = f"{CONSUMER_ID}\n{timestamp}\n{KEY_VERSION}\n"

    rsakey = RSA.importKey(rsa_pem)
    signer = PKCS1_v1_5.new(rsakey)
    digest = SHA256.new()
    digest.update(data.encode("utf-8"))
    signature = base64.b64encode(signer.sign(digest)).decode("utf-8")

    return {
        "WM_CONSUMER.ID": CONSUMER_ID,
        "WM_SVC.NAME": "WMTLLMGATEWAY",
        "WM_SVC.ENV": "stage",
        "WM_SEC.KEY_VERSION": str(KEY_VERSION),
        "WM_SEC.AUTH_SIGNATURE": signature,
        "WM_CONSUMER.INTIMESTAMP": str(timestamp),
        "Content-Type": "application/json",
        "wm_llm_gw.user_type": "ASSOCIATE",
        "wm_llm_gw.user_name": associate_id,
    }


# ── LLM call ─────────────────────────────────────────────────────────────────

def call_elementai(prompt: str, user_bearer_token: str, associate_id: str = "", max_tokens: int = 2048, temperature: float = 0.1) -> str:
    """
    Call the Walmart LLM Gateway via the Google GenAI endpoint.
    Uses RSA-signed WM headers.
    associate_id: caller's Walmart associate ID — required by the gateway.
    """
    endpoint = WMT_GATEWAY_URL.rstrip("/") + "/v1/google-genai"

    try:
        headers = _get_gateway_headers(associate_id)
    except Exception as e:
        raise ValueError(f"Failed to build gateway auth headers: {e}")

    # Google GenAI payload format — prompt includes all system context from the caller
    payload = {
        "model": LLM_MODEL,
        "model-version": LLM_MODEL_VERSION or "001",
        "task": "generateContent",
        "model-params": {
            "contents": {
                "role": "user",
                "parts": {"text": prompt},
            },
            "generation_config": {
                "temperature": 0.1,
                "maxOutputTokens": max_tokens,
                "temperature": temperature,
            },
        },
    }

    r = requests.post(endpoint, headers=headers, json=payload, timeout=60)

    if r.status_code != 200:
        raise ValueError(f"LLM Gateway returned HTTP {r.status_code}: {r.text[:300]}")

    data = r.json()

    # Extract text from Google GenAI response structure
    try:
        candidates = data.get("candidates", [])
        if candidates:
            cand = candidates[0]
            content = cand.get("content") or cand.get("output", {})
            if isinstance(content, dict):
                parts = content.get("parts", [])
                texts = [p["text"] for p in parts if isinstance(p, dict) and "text" in p]
                if texts:
                    return "\n".join(texts)
            # fallback: flat text/output field on candidate
            for key in ("text", "output", "message"):
                if key in cand and isinstance(cand[key], str):
                    return cand[key]
    except Exception:
        pass

    raise ValueError(f"Could not extract text from gateway response: {str(data)[:300]}")


# ── Prompt builder ────────────────────────────────────────────────────────────

def _duration_str(start: str, end: str) -> str:
    """Compute human-readable duration from ISO date strings."""
    try:
        fmt = "%Y-%m-%d %H:%M:%S"
        s = datetime.strptime(start, fmt)
        e = datetime.strptime(end, fmt)
        diff = int((e - s).total_seconds())
        return f"{diff // 60}m {diff % 60}s"
    except Exception:
        return "unknown"


def build_diagnosis_prompt(
    run: dict,
    workflow: dict,
    notebook_ctx: dict,
    job_history: list[dict],
    err_log: str,
    cluster: dict,
) -> str:
    """Assemble all collected context into a structured prompt for ElementAI."""

    wf_name = (
        run.get("workflow", {}).get("workflowName", "")
        if isinstance(run.get("workflow"), dict)
        else ""
    ) or workflow.get("workflowName", "unknown")

    start = run.get("scheduleStartDate", "")
    end   = run.get("scheduleEndDate", "")
    duration = _duration_str(start, end) if start and end else "unknown"

    spark = workflow.get("sparkConfig", {})
    errors = notebook_ctx.get("errors", {})

    # ── Section: run details ──
    run_section = f"""
=== FAILED JOB DETAILS ===
Workflow name  : {wf_name}
Script         : {workflow.get('mainScript', 'unknown')}
Schedule ID    : {run.get('scheduleMetaId')}
Started        : {start}
Duration       : {duration}
Status         : {run.get('scheduleStatus')}
"""

    # ── Section: cluster config ──
    config_section = f"""
=== CLUSTER CONFIGURATION ===
Executors            : {spark.get('numExecutors', 'unknown')}
Executor memory      : {spark.get('executorMemoryMB', 'unknown')} MB per executor
Driver memory        : {spark.get('driverMemoryMB', 'unknown')} MB
Dynamic allocation   : {spark.get('dynamicAllocationEnabled', 'unknown')}
Autoscale enabled    : {spark.get('autoscaleEnabled', 'unknown')}
VM family            : {spark.get('vmFamily', 'unknown')}
Cluster uptime       : {cluster.get('uptime', 'unknown')}
"""

    # ── Section: job history pattern ──
    failed_runs   = [r for r in job_history if r.get("scheduleStatus") == "FAILED"]
    success_runs  = [r for r in job_history if r.get("scheduleStatus") == "SUCCESS"]
    recent_3      = [r.get("scheduleStatus") for r in job_history[:3]]
    failure_rate  = f"{len(failed_runs) / len(job_history) * 100:.0f}%" if job_history else "unknown"

    history_section = f"""
=== JOB HISTORY PATTERN ===
Total runs     : {len(job_history)}
Successful     : {len(success_runs)}
Failed         : {len(failed_runs)}
Failure rate   : {failure_rate}
Last 3 statuses: {recent_3}
"""

    # ── Section: errors from notebook output ──
    failing_cells = notebook_ctx.get("failingCells", [])
    error_section = ""
    cells_section = ""

    if failing_cells:
        error_section = "\n=== ERRORS FROM NOTEBOOK OUTPUT ===\n"
        error_section += f"({len(failing_cells)} cell(s) with errors — source and error shown together)\n"
        for fc in failing_cells:
            error_section += (
                f"\n[Cell {fc['cell_num']}] ERROR — {fc['error_type']}:\n"
                f"Source code:\n{fc['source']}\n"
                f"Error message: {fc['error_text']}\n"
            )
    elif errors.get("has_errors"):
        error_section = f"""
=== ERRORS FOUND IN NOTEBOOK OUTPUT ===
{errors.get('all_exceptions_text', '')}
"""
        cell_sources = notebook_ctx.get("cellSources", [])
        if cell_sources:
            cells_section = f"\n=== NOTEBOOK CELL CODE (first {min(6, len(cell_sources))} cells) ===\n"
            for i, src in enumerate(cell_sources[:6], 1):
                cells_section += f"\n[Cell {i}]:\n{src}\n"
    elif err_log:
        error_section = f"""
=== JOB RUNNER ERROR LOG ===
{err_log[:600]}
"""
    else:
        error_section = "\n=== ERRORS ===\nNo error output captured.\n"

    # ── Compose final prompt ──
    prompt = f"""You are diagnosing a failed PySpark notebook job on the Walmart Element ML Platform.

The data scientist wants to know: what failed, why, and exactly what to fix.

{run_section}
{config_section}
{history_section}
{error_section}
{cells_section}
Your diagnosis must:
1. Name the EXACT root cause (specific error, variable, column, or config issue)
2. Say whether this is a CODE issue, DATA issue, or INFRASTRUCTURE issue
3. Give the specific fix (what to change, where)
4. Note any relevant pattern from the job history
5. If the cluster config contributed to the failure, say so specifically

Be direct and precise. Do not say "check the logs" or "investigate further" — give the answer now."""

    return prompt.strip()


# ── Main diagnosis function ───────────────────────────────────────────────────

def diagnose_failure(
    schedule_meta_id: int,
    project_id: int,
    workflow_meta_id: int,
    spark_job_id: int,
    bearer_token: str,
    cookies: dict,
) -> dict:
    """
    Full failure diagnosis pipeline.

    Args:
        schedule_meta_id : scheduleMetaId of the failed run (from job list)
        project_id       : Element ML project ID
        workflow_meta_id : workflowMetaId (from job list)
        spark_job_id     : Spark job ID on port 31001 (from cost dashboard / node run)
        bearer_token     : User's SSO Bearer token
        cookies          : Session cookies dict

    Returns:
        Structured dict with diagnosis, errors found, spark config, history summary.
    """
    client = ElementMLClient(bearer_token, cookies)

    # 1. Job history + locate this run
    job_history = client.get_job_history(project_id)
    target_run = next(
        (r for r in job_history if r.get("scheduleMetaId") == schedule_meta_id),
        None,
    )
    if not target_run:
        raise ValueError(
            f"Run {schedule_meta_id} not found in job history for project {project_id}."
        )

    # 2. Workflow definition (spark config + script name)
    workflow = client.get_workflow_definition(workflow_meta_id, project_id)

    # 3. Cluster details (status + batchId for log paths)
    cluster: dict = {}
    batch_id = workflow.get("notebookId")  # fallback: notebookId == batchId
    try:
        cluster = client.get_cluster_details(spark_job_id)
        if cluster.get("batchId"):
            batch_id = cluster["batchId"]
    except Exception:
        pass

    # 4. Rendered notebook HTML — primary source of actual errors
    notebook_ctx: dict = {"errors": {"exceptions": [], "warnings": [], "has_errors": False, "all_exceptions_text": ""}, "cellSources": [], "cellCount": 0}
    if batch_id:
        try:
            html = client.get_rendered_notebook_html(spark_job_id, int(batch_id))
            notebook_ctx = build_notebook_context(html)
        except Exception:
            pass

    # 5. .err log (fallback / supplementary)
    err_log = ""
    if batch_id:
        try:
            err_log = client.get_error_log(spark_job_id, int(batch_id))
        except Exception:
            pass

    # 6. Build prompt + call LLM
    prompt = build_diagnosis_prompt(
        run=target_run,
        workflow=workflow,
        notebook_ctx=notebook_ctx,
        job_history=job_history,
        err_log=err_log,
        cluster=cluster,
    )

    diagnosis_text = call_elementai(prompt, bearer_token)

    # 7. Return structured response
    failed_runs  = [r for r in job_history if r.get("scheduleStatus") == "FAILED"]
    success_runs = [r for r in job_history if r.get("scheduleStatus") == "SUCCESS"]

    return {
        "scheduleMetaId": schedule_meta_id,
        "status": target_run.get("scheduleStatus"),
        "workflowName": workflow.get("workflowName"),
        "mainScript": workflow.get("mainScript"),
        "errorsFound": notebook_ctx["errors"].get("exceptions", []),
        "warnings": notebook_ctx["errors"].get("warnings", []),
        "sparkConfig": workflow.get("sparkConfig", {}),
        "clusterUptime": cluster.get("uptime"),
        "diagnosis": diagnosis_text,
        "jobHistorySummary": {
            "totalRuns": len(job_history),
            "failedRuns": len(failed_runs),
            "successRuns": len(success_runs),
            "failureRate": f"{len(failed_runs)/len(job_history)*100:.0f}%" if job_history else "N/A",
            "recentStatuses": [r.get("scheduleStatus") for r in job_history[:5]],
        },
    }
