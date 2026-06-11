# SparkNurse 🩺

> A conversational AI assistant that integrates with the Element ML workflow to eliminate the job failure loop — diagnosing failures before engineers waste hours hunting logs, catching code issues before they cost compute, and tracking ongoing jobs in the background.

---

## The Problem

Data scientists working with big data on Element ML lose hours manually debugging PySpark job failures. When a job fails, there's no immediate answer — engineers scroll through notebook HTML, hunt error traces, and cross-reference logs.

In many cases these are common, fixable errors. In others, they're not easy fixes — leading to failed retrials, long diagnostic discussions with more experienced scientists, or copy-pasting logs into external LLMs. Either way, it's time and compute wasted on work that shouldn't need to happen.

SparkNurse was built by a data scientist, for data scientists.

---

## What SparkNurse Does

### 🔍 Failure Diagnosis
When a job fails, SparkNurse tells you the exact cell the error occurred in, what the exact error is, and the exact fix — no log hunting. Reduction in failed retrials means direct compute savings.

### 🔔 Background Job Tracking
You don't need to wait for a failure to use SparkNurse. Submit a job, tell SparkNurse, and go do other work. It monitors the job in the background and notifies you the moment it succeeds or fails — with full diagnosis included.

Element ML already sends failure emails, but those emails don't include diagnostics. SparkNurse does.

### 📎 Pre-Run Notebook Review
Attach a `.ipynb` notebook and ask SparkNurse to review it before you run it. It catches syntax errors, bad column references, incorrect table paths, missing imports, schema mismatches, and PySpark anti-patterns (missing broadcast hints, OOM risks, UDF inefficiencies) — before a single cluster spins up.

---

## Impact — Track T04-02: Infra Utilization

- Reduces mean time to diagnose complex failures from hours to seconds
- Prevents wasted re-runs from fixable failures
- Directly reduces compute spend on the Element ML platform
- Compresses the developer debugging loop from hours to seconds — engineering time back to actual work

Both dimensions compound at scale. The more data scientists use Element ML, the greater the aggregate return from shifting failure diagnosis from manual effort to automated intelligence.

---

## Tech Stack

- **Backend:** Python, FastAPI, uvicorn
- **LLM:** Walmart LLM Gateway via `call_elementai()`
- **Platform:** Element ML (`ml.prod.walmart.com`) — live API integration against real job data, no mocks. Currently runs as a standalone agent; embedding directly into the Element ML UI is the next step.
- **Frontend:** Vanilla HTML/CSS/JS, single-page chat UI

---

## Setup & Running

### Prerequisites
- Python 3.11+
- Access to Element ML (`ml.prod.walmart.com`)
- Walmart SSO Bearer token + Cookie string (from browser DevTools)

### Install
```bash
git clone <repo-url>
cd element_ml_agent
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Run
```bash
uvicorn main:app --reload --port 8000
```

Open `http://localhost:8000` in your browser.

### Login
On first load, enter your:
- **Walmart Associate ID** (e.g. `o0o01od`)
- **Bearer token** — DevTools → Network → any request to `ml.prod.walmart.com` → `Authorization` header
- **Cookie string** — same request → `Cookie` header

---

## Project Structure

```
agent.py          # Walmart LLM Gateway client
chat_agent.py     # Conversational logic, intent detection, job context
data_layer.py     # Element ML API client
failure_poller.py # Job tracking + failure diagnosis engine
html_parser.py    # Notebook HTML error extraction
main.py           # FastAPI server + endpoints
static/           # Chat UI (index.html)
```

---

## Current State & Roadmap

SparkNurse is currently a standalone agent that integrates with Element ML's live APIs and is currently being deployed. The immediate next step is to simplify authentication — replacing the Bearer token and Cookie string with a standard Walmart Associate ID and password login. The longer-term goal is to collaborate with the Element ML team to embed SparkNurse directly into the platform UI, making it natively available to all data scientists without a separate setup.

---

## Hackathon

**Global Tech Hackathon 2026 — Everybody Hacks**
Track T04-02 · Infra Utilization · Team: o0o01od, j0w0vpy
