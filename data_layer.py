"""
data_layer.py — Element ML API client.

All endpoints confirmed via probe scripts against ml.prod.walmart.com.
Auth: SSO Bearer token + session cookies (refresh from DevTools ~every 12h).
"""
import json
import re
import urllib3
import requests
from datetime import datetime

urllib3.disable_warnings()

API_BASE = "https://ml.prod.walmart.com:31200"
JOB_BASE = "https://ml.prod.walmart.com:31001"


class ElementMLClient:
    def __init__(self, bearer_token: str, cookies: dict):
        token = bearer_token if bearer_token.startswith("Bearer ") else f"Bearer {bearer_token}"
        self.headers = {
            "Authorization": token,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        self.cookies = cookies

    # ── Job history ──────────────────────────────────────────────────────────

    def get_job_history(self, project_id: int) -> list[dict]:
        """
        Returns all schedule runs for a project.
        KILLED runs are excluded (those are intentional user cancellations).
        Confirmed working: returns 39 runs for project 16701.
        """
        r = requests.get(
            f"{API_BASE}/v1/workflows/schedules?projectId={project_id}",
            headers=self.headers,
            cookies=self.cookies,
            verify=False,
            timeout=15,
        )
        r.raise_for_status()
        runs = r.json()
        return [run for run in runs if run.get("scheduleStatus") != "KILLED"]

    # ── Workflow definition ──────────────────────────────────────────────────

    def get_workflow_definition(self, workflow_id: int, project_id: int) -> dict:
        """
        Returns the workflow definition including:
          - notebook_id, mainScript (.ipynb filename)
          - Full sparkResources (numExecutors, executorMemory, dynamicAllocation, etc.)
        Confirmed working: GET /v1/workflows/48227?projectId=16701.
        """
        r = requests.get(
            f"{API_BASE}/v1/workflows/{workflow_id}?projectId={project_id}",
            headers=self.headers,
            cookies=self.cookies,
            verify=False,
            timeout=15,
        )
        r.raise_for_status()
        workflow = r.json()

        wc_raw = workflow.get("workflowContent", "{}")
        wc = json.loads(wc_raw) if isinstance(wc_raw, str) else wc_raw

        tasks = wc.get("tasks", {})
        node = tasks.get("node_1", {}) if isinstance(tasks, dict) else {}

        resources = node.get("resources", {})
        spark = resources.get("sparkResources", {})

        configs_raw = resources.get("configs", "{}")
        configs = json.loads(configs_raw) if isinstance(configs_raw, str) else (configs_raw or {})

        main_script = (
            node.get("main_script_name", {}).get("value")
            or node.get("ui_specific_info", {}).get("main_script_name")
        )
        notebook_id = node.get("notebook_id", {}).get("value")

        return {
            "workflowName": workflow.get("workflowName"),
            "workflowMetaId": workflow.get("workflowMetaId"),
            "notebookId": notebook_id,
            "mainScript": main_script,
            "sparkConfig": {
                "numExecutors": spark.get("sparkNumExecutors"),
                "executorCores": spark.get("sparkExecutorCores"),
                "executorMemoryMB": spark.get("sparkExecutorMemory"),
                "driverMemoryMB": spark.get("sparkDriverMemory"),
                "autoscaleEnabled": spark.get("sparkAutoscaleEnabled"),
                "vmFamily": spark.get("sparkVmFamily"),
                "dynamicAllocationEnabled": configs.get(
                    "spark.dynamicAllocation.enabled", "unknown"
                ),
            },
        }

    # ── Schedule detail ──────────────────────────────────────────────────────

    def get_schedule_detail(self, schedule_meta_id: int) -> dict:
        """
        Returns full schedule record including workflowDagId.
        Confirmed working: GET /v1/workflows/schedules/{scheduleMetaId} → 200.
        """
        r = requests.get(
            f"{API_BASE}/v1/workflows/schedules/{schedule_meta_id}",
            headers=self.headers,
            cookies=self.cookies,
            verify=False,
            timeout=15,
        )
        r.raise_for_status()
        return r.json()

    # ── Resolve spark_job_id via Airflow task logs ───────────────────────────

    def find_spark_job_id(
        self,
        batch_id: int,
        schedule_meta_id: int | None = None,
        workflow_dag_id: str | None = None,
        run_index: int = 0,
        target_execution_date: str | None = None,
    ) -> int | None:
        """
        Discover spark_job_id automatically via the Airflow REST API chain.
        run_index=0 → most recent run, run_index=1 → second most recent, etc.

        target_execution_date: scheduleStartDate of the specific run to find
          (YYYY-MM-DD HH:MM:SS or ISO format). When provided, the function
          tries an Airflow date-filter first, then matches the closest dag_run.
          Falls back to limit=30 window if date-filter is unsupported.

        Confirmed working chain (all endpoints probed and validated):
          1. GET :31200/v1/workflows/dag/{dagId}/dagUrl?type=dagRun
             → {"astro": true, "url": "https://...31206/{slug}/airflow/dagrun/list/..."}
          2. Parse Airflow deployment slug from URL.
          3. GET :31206/{slug}/airflow/api/v1/dags/{dagId}/dagRuns?limit=5&order_by=-execution_date
             → recent dag runs with dag_run_id
          4. GET .../dagRuns/{run_id}/taskInstances
             → task list with task_id + try_number + state
          5. GET .../taskInstances/{task_id}/logs/{try_number}
             → plain-text log containing {"runId":XXXXXX} = spark_job_id

        batch_id is accepted for signature compatibility but not used in the chain —
        the runId in task logs directly identifies the spark job.
        """
        from urllib.parse import quote

        if not workflow_dag_id:
            return None

        # Step 1: Get Airflow deployment URL via dagUrl endpoint
        try:
            dagurl_resp = requests.get(
                f"{API_BASE}/v1/workflows/dag/{workflow_dag_id}/dagUrl?type=dagRun",
                headers=self.headers,
                cookies=self.cookies,
                verify=False,
                timeout=15,
            )
        except Exception:
            return None

        if dagurl_resp.status_code != 200:
            return None

        try:
            airflow_ui_url = dagurl_resp.json().get("url", "")
        except Exception:
            return None

        # Step 2: Parse Airflow deployment slug
        # URL format: https://ml.prod.walmart.com:31206/{slug}/airflow/dagrun/list/?...
        slug_match = re.search(r":\d+(/[^/]+/airflow)", str(airflow_ui_url))
        if not slug_match:
            return None
        airflow_path = slug_match.group(1)  # e.g. "/sidereal-ecliptic-9184/airflow"
        airflow_base = f"https://ml.prod.walmart.com:31206{airflow_path}/api/v1"

        # Step 3: Get dag runs — try date-filter first when target_execution_date provided
        dag_runs = []

        if target_execution_date:
            target_day = str(target_execution_date)[:10]
            try:
                date_resp = requests.get(
                    f"{airflow_base}/dags/{workflow_dag_id}/dagRuns"
                    f"?limit=90&order_by=-execution_date"
                    f"&execution_date_gte={target_day}T00:00:00+00:00"
                    f"&execution_date_lte={target_day}T23:59:59+00:00",
                    headers=self.headers,
                    cookies=self.cookies,
                    verify=False,
                    timeout=20,
                )
                if date_resp.status_code == 200:
                    dag_runs = date_resp.json().get("dag_runs", [])
            except Exception:
                pass

        if not dag_runs:
            # Fallback: fetch last 90 runs
            try:
                runs_resp = requests.get(
                    f"{airflow_base}/dags/{workflow_dag_id}/dagRuns"
                    f"?limit=90&order_by=-execution_date",
                    headers=self.headers,
                    cookies=self.cookies,
                    verify=False,
                    timeout=20,
                )
            except Exception:
                return None

            if runs_resp.status_code != 200:
                return None

            try:
                dag_runs = runs_resp.json().get("dag_runs", [])
            except Exception:
                return None

        if not dag_runs:
            return None

        # Determine which runs to walk
        if target_execution_date:
            # Match dag_run closest to target_execution_date (within 2-hour window)
            try:
                target_dt = datetime.fromisoformat(
                    str(target_execution_date).replace(" ", "T").split("+")[0].rstrip("Z")
                )

                def _run_distance(run):
                    exec_date = run.get("execution_date", "")
                    try:
                        rd = datetime.fromisoformat(
                            exec_date.rstrip("Z").split("+")[0]
                        )
                        return abs((rd - target_dt).total_seconds())
                    except Exception:
                        return float("inf")

                dag_runs_sorted = sorted(dag_runs, key=_run_distance)
                best_dist = _run_distance(dag_runs_sorted[0])
                if best_dist > 43200:  # no match within 12 hours → run not in window
                    return None
                runs_to_walk = dag_runs_sorted[:3]
            except Exception:
                runs_to_walk = dag_runs[:3]
        else:
            runs_to_walk = dag_runs[run_index:run_index + 3]

        # Steps 4–5: Walk runs → task instances → logs → extract runId
        for run in runs_to_walk:
            dag_run_id_enc = quote(run.get("dag_run_id", ""), safe="")
            if not dag_run_id_enc:
                continue

            try:
                ti_resp = requests.get(
                    f"{airflow_base}/dags/{workflow_dag_id}"
                    f"/dagRuns/{dag_run_id_enc}/taskInstances",
                    headers=self.headers,
                    cookies=self.cookies,
                    verify=False,
                    timeout=15,
                )
            except Exception:
                continue

            if ti_resp.status_code != 200:
                continue

            try:
                tasks = ti_resp.json().get("task_instances", [])
            except Exception:
                continue

            # Prefer failed tasks; fall back to all tasks
            failed_tasks = [t for t in tasks if t.get("state") == "failed"]
            ordered_tasks = failed_tasks or tasks

            for task in ordered_tasks:
                task_id = task.get("task_id")
                try_number = task.get("try_number", 1)
                if not task_id:
                    continue

                try:
                    log_resp = requests.get(
                        f"{airflow_base}/dags/{workflow_dag_id}"
                        f"/dagRuns/{dag_run_id_enc}"
                        f"/taskInstances/{task_id}/logs/{try_number}",
                        headers={**self.headers, "Accept": "text/plain"},
                        cookies=self.cookies,
                        verify=False,
                        timeout=30,
                    )
                except Exception:
                    continue

                if log_resp.status_code != 200:
                    continue

                match = re.search(r'"runId"\s*:\s*(\d+)', log_resp.text)
                if match:
                    return int(match.group(1))

        return None

    # ── Full Airflow task log (for cell-source inspection) ───────────────────

    def get_airflow_task_log_full(
        self,
        workflow_dag_id: str,
        run_index: int = 0,
        max_chars: int = 80_000,
    ) -> dict:
        """
        Return the full Airflow task log text for the most recent run.

        Used to investigate whether Papermill logs cell source code,
        which would give us pre-run notebook source via the working auth chain.

        Returns:
            {
                "airflow_base": str,
                "dag_run_id": str,
                "task_id": str,
                "try_number": int,
                "log_chars": int,
                "has_runId": bool,
                "has_cells_keyword": bool,
                "log_text": str (first max_chars chars),
            }
        or raises on auth/network failure.
        """
        from urllib.parse import quote

        # Step 1: dagUrl
        dagurl_resp = requests.get(
            f"{API_BASE}/v1/workflows/dag/{workflow_dag_id}/dagUrl?type=dagRun",
            headers=self.headers,
            cookies=self.cookies,
            verify=False,
            timeout=15,
        )
        dagurl_resp.raise_for_status()
        airflow_ui_url = dagurl_resp.json().get("url", "")

        # Step 2: slug
        slug_match = re.search(r":\d+(/[^/]+/airflow)", str(airflow_ui_url))
        if not slug_match:
            raise ValueError(f"Cannot parse Airflow slug from URL: {airflow_ui_url!r}")
        airflow_base = f"https://ml.prod.walmart.com:31206{slug_match.group(1)}/api/v1"

        # Step 3: recent dag runs
        runs_resp = requests.get(
            f"{airflow_base}/dags/{workflow_dag_id}/dagRuns"
            f"?limit=5&order_by=-execution_date",
            headers=self.headers,
            cookies=self.cookies,
            verify=False,
            timeout=20,
        )
        runs_resp.raise_for_status()
        dag_runs = runs_resp.json().get("dag_runs", [])
        if not dag_runs or run_index >= len(dag_runs):
            raise ValueError(f"No dag run at index {run_index}")

        run = dag_runs[run_index]
        dag_run_id = run.get("dag_run_id", "")
        dag_run_id_enc = quote(dag_run_id, safe="")

        # Step 4: task instances
        ti_resp = requests.get(
            f"{airflow_base}/dags/{workflow_dag_id}"
            f"/dagRuns/{dag_run_id_enc}/taskInstances",
            headers=self.headers,
            cookies=self.cookies,
            verify=False,
            timeout=15,
        )
        ti_resp.raise_for_status()
        tasks = ti_resp.json().get("task_instances", [])
        if not tasks:
            raise ValueError("No task instances found")

        # Pick the most interesting task (failed first, otherwise first)
        failed_tasks = [t for t in tasks if t.get("state") == "failed"]
        task = (failed_tasks or tasks)[0]
        task_id = task.get("task_id")
        try_number = task.get("try_number", 1)

        # Step 5: full log
        log_resp = requests.get(
            f"{airflow_base}/dags/{workflow_dag_id}"
            f"/dagRuns/{dag_run_id_enc}"
            f"/taskInstances/{task_id}/logs/{try_number}",
            headers={**self.headers, "Accept": "text/plain"},
            cookies=self.cookies,
            verify=False,
            timeout=60,
        )
        log_resp.raise_for_status()
        log_text = log_resp.text

        return {
            "airflow_base": airflow_base,
            "dag_run_id": dag_run_id,
            "task_id": task_id,
            "try_number": try_number,
            "log_chars": len(log_text),
            "has_runId": bool(re.search(r'"runId"\s*:\s*\d+', log_text)),
            "has_cells_keyword": "Executing notebook with kernel" in log_text or
                                  '"cell_type"' in log_text or
                                  "cell_source" in log_text.lower() or
                                  "Executing cell" in log_text,
            "log_text": log_text[:max_chars],
        }

    # ── Cluster details from port 31001 ──────────────────────────────────────

    def get_cluster_details(self, spark_job_id: int) -> dict:
        """
        Fetches job detail page from port 31001.
        Returns cluster status, uptime, and batchId (needed for log file paths).
        Confirmed working: sparkJobId=27801126 → Status=DELETED, Uptime=18min53s.
        """
        r = requests.get(
            f"{JOB_BASE}/v1/jobs/{spark_job_id}",
            headers={**self.headers, "Accept": "text/html"},
            cookies=self.cookies,
            verify=False,
            timeout=15,
        )
        r.raise_for_status()
        html = r.text

        pairs = re.findall(r"<td>([^<]+)</td>\s*<td>([^<]+)</td>", html)
        info = {k.strip(): v.strip() for k, v in pairs}

        err_match = re.search(r"batch-(\d+)-\d+\.err", html)
        batch_id = err_match.group(1) if err_match else None

        return {
            "status": info.get("Status"),
            "uptime": info.get("ApproxUptime"),
            "batchId": batch_id,
            "rawInfo": info,
        }

    # ── Rendered notebook HTML (primary diagnosis source) ────────────────────

    def get_rendered_notebook_html(self, spark_job_id: int, batch_id: int) -> str:
        """
        Fetches the fully executed notebook as HTML.
        This contains cell source code AND actual error outputs (NameError, AnalysisException, etc.).
        Confirmed working: returns 307-338K chars for sparkJobId=27801126.
        """
        url = f"{JOB_BASE}/v1/jobs/{spark_job_id}/batch-{batch_id}-{spark_job_id}.html"
        r = requests.get(
            url,
            headers={**self.headers, "Accept": "text/html"},
            cookies=self.cookies,
            verify=False,
            timeout=30,
        )
        r.raise_for_status()
        return r.text

    # ── Error log (secondary/fallback) ───────────────────────────────────────

    def get_error_log(self, spark_job_id: int, batch_id: int) -> str:
        """
        Fetches the .err stderr file for a job run.
        Contains the job runner wrapper traceback (shallow — use rendered HTML for root cause).
        Confirmed working: returns CellExecutionError traceback.
        """
        url = f"{JOB_BASE}/v1/jobs/{spark_job_id}/batch-{batch_id}-{spark_job_id}.err"
        r = requests.get(
            url,
            headers=self.headers,
            cookies=self.cookies,
            verify=False,
            timeout=15,
        )
        if r.status_code == 200:
            return r.text.strip()
        return ""

    # ── Notebook source (.ipynb) ─────────────────────────────────────────────

    def get_notebook_source(self, notebook_id: int, project_id: int, workflow_id: int | None = None) -> dict | None:
        """
        Fetch the raw .ipynb notebook source for pre-run code review and failure inference.
        Returns parsed JSON dict (Jupyter notebook format) or None if unavailable.

        Tries multiple endpoint patterns — the working one will be confirmed via debug log.
        """
        import sys
        candidates = [
            f"{API_BASE}/v1/notebooks/{notebook_id}?projectId={project_id}",
            f"{API_BASE}/v1/files/{notebook_id}?projectId={project_id}",
            f"{API_BASE}/v1/notebook-files/{notebook_id}?projectId={project_id}",
            f"{API_BASE}/v1/notebooks/{notebook_id}",
            f"{API_BASE}/v1/notebooks?notebookId={notebook_id}&projectId={project_id}",
        ]
        if workflow_id:
            candidates += [
                f"{API_BASE}/v1/workflows/{workflow_id}/notebook?projectId={project_id}",
                f"{API_BASE}/v1/notebooks?workflowId={workflow_id}&projectId={project_id}",
            ]

        for url in candidates:
            try:
                r = requests.get(url, headers=self.headers, cookies=self.cookies, verify=False, timeout=15)
                print(f"[DEBUG get_notebook_source] {r.status_code} {url} | body[:200]: {r.text[:200]}", file=sys.stderr)
                if r.status_code == 200:
                    text = r.text.strip()
                    # Try to parse as JSON notebook
                    try:
                        nb = json.loads(text)
                        if isinstance(nb, dict) and ("cells" in nb or "nbformat" in nb):
                            print(f"[DEBUG get_notebook_source] ✅ Got notebook with {len(nb.get('cells', []))} cells from {url}", file=sys.stderr)
                            return nb
                        # Maybe it's wrapped: {"notebook": {...}} or {"content": "..."}
                        for key in ("notebook", "content", "data", "file"):
                            val = nb.get(key) if isinstance(nb, dict) else None
                            if isinstance(val, dict) and "cells" in val:
                                print(f"[DEBUG get_notebook_source] ✅ Got notebook (wrapped .{key}) from {url}", file=sys.stderr)
                                return val
                            if isinstance(val, str):
                                try:
                                    inner = json.loads(val)
                                    if "cells" in inner:
                                        print(f"[DEBUG get_notebook_source] ✅ Got notebook (string .{key}) from {url}", file=sys.stderr)
                                        return inner
                                except Exception:
                                    pass
                    except Exception:
                        pass
            except Exception as e:
                print(f"[DEBUG get_notebook_source] error {url}: {e}", file=sys.stderr)

        return None
