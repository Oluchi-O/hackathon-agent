"""
html_parser.py — Parse rendered Jupyter notebook HTML to extract diagnostic data.

The rendered notebook HTML (from port 31001) contains the fully executed notebook:
  - Every cell's source code
  - Every cell's output (including actual Python/Spark exceptions)
  - Warnings

Confirmed: NameError: name 'd_df' is not defined was extracted from a failed run.
"""
import re


# Exception types to detect — ordered from most specific to most general
EXCEPTION_PATTERNS = [
    (r"pyspark\.sql\.utils\.AnalysisException[^\n<]{0,500}", "AnalysisException"),
    (r"pyspark\.sql\.utils\.ParseException[^\n<]{0,300}", "ParseException"),
    (r"Py4JJavaError[^\n<]{0,500}", "Py4JJavaError"),
    (r"java\.lang\.OutOfMemoryError[^\n<]{0,300}", "OutOfMemoryError"),
    (r"java\.lang\.[A-Za-z]+Exception[^\n<]{0,300}", "JavaException"),
    (r"NameError:\s*name\s+'[^']+'\s+is not defined[^\n<]{0,200}", "NameError"),
    (r"AttributeError[^\n<]{0,300}", "AttributeError"),
    (r"KeyError[^\n<]{0,200}", "KeyError"),
    (r"TypeError[^\n<]{0,300}", "TypeError"),
    (r"ValueError[^\n<]{0,300}", "ValueError"),
    (r"ImportError[^\n<]{0,200}", "ImportError"),
    (r"ModuleNotFoundError[^\n<]{0,200}", "ModuleNotFoundError"),
    (r"IndexError[^\n<]{0,200}", "IndexError"),
    (r"FileNotFoundError[^\n<]{0,200}", "FileNotFoundError"),
    (r"PermissionError[^\n<]{0,200}", "PermissionError"),
    (r"nbclient\.exceptions\.[^\n<]{0,200}", "NBClientException"),
]

WARNING_PATTERNS = [
    r"SyntaxWarning[^\n<]{0,200}",
    r"UserWarning[^\n<]{0,200}",
    r"DeprecationWarning[^\n<]{0,200}",
    r"RuntimeWarning[^\n<]{0,200}",
]


def _clean_html(text: str) -> str:
    """Strip HTML tags and normalize whitespace."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("&lt;", "<").replace("&gt;", ">")
    text = text.replace("&amp;", "&").replace("&#39;", "'").replace("&quot;", '"')
    return re.sub(r"\s+", " ", text).strip()


def extract_errors(html: str) -> dict:
    """
    Extract all errors and warnings from a rendered notebook HTML.

    Returns:
        {
            "exceptions": [{"type": str, "message": str}, ...],
            "warnings": [str, ...],
            "all_exceptions_text": str,   # combined text for LLM prompt
            "has_errors": bool
        }
    """
    exceptions = []
    seen_messages = set()

    for pattern, err_type in EXCEPTION_PATTERNS:
        matches = re.findall(pattern, html, re.IGNORECASE | re.DOTALL)
        for m in matches:
            clean = _clean_html(m)[:350]
            if clean and len(clean) > 15 and clean not in seen_messages:
                seen_messages.add(clean)
                exceptions.append({"type": err_type, "message": clean})

    warnings = []
    for pattern in WARNING_PATTERNS:
        matches = re.findall(pattern, html, re.IGNORECASE)
        for m in matches:
            clean = _clean_html(m)[:200]
            if clean and clean not in warnings:
                warnings.append(clean)

    all_text_parts = [f"{e['type']}: {e['message']}" for e in exceptions]
    all_text_parts += [f"Warning: {w}" for w in warnings]

    return {
        "exceptions": exceptions,
        "warnings": warnings,
        "all_exceptions_text": "\n".join(all_text_parts),
        "has_errors": len(exceptions) > 0,
    }


def extract_cell_sources(html: str, max_cells: int = 15) -> list[str]:
    """
    Extract code cell source text from rendered notebook HTML.
    Uses <pre> blocks which contain syntax-highlighted cell source.

    Returns a list of code strings (up to max_cells cells).
    """
    pre_blocks = re.findall(r"<pre>(.*?)</pre>", html, re.DOTALL)

    sources = []
    for block in pre_blocks:
        clean = _clean_html(block)
        # Filter out CSS/style snippets — real code cells are longer and don't start with '.'
        if (
            clean
            and len(clean) > 20
            and not clean.startswith(".")
            and not clean.startswith("{")
            and "color:" not in clean[:50]
        ):
            sources.append(clean[:600])
            if len(sources) >= max_cells:
                break

    return sources


def extract_failing_cells(html: str) -> list[dict]:
    """
    Find cells that have error output, returning
    [{cell_num, source, error_type, error_text}].

    Strategy 1 (preferred): segment HTML by execution-count markers
    ("In [N]:" from classic nbconvert or "[N]:" from JupyterLab), then
    scan each cell segment for exceptions.

    Strategy 2 (fallback): position-based — for each exception match,
    find the nearest preceding filtered <pre> block.
    """
    results = []

    # ── Strategy 1: segment by execution-count markers ──────────────────────
    # Handles: "In [5]:", "In&nbsp;[5]:", "[5]:" (JupyterLab)
    exec_re = re.compile(r'In\s*(?:&nbsp;)?\[(\d+)\]:', re.IGNORECASE)
    markers = list(exec_re.finditer(html))

    # Also try JupyterLab format if classic not found
    if not markers:
        exec_re2 = re.compile(r'\[(\d+)\]:', re.IGNORECASE)
        markers = [m for m in exec_re2.finditer(html) if m.group(1).isdigit()]

    if markers:
        for i, m in enumerate(markers):
            seg_start = m.start()
            seg_end = markers[i + 1].start() if i + 1 < len(markers) else len(html)
            seg = html[seg_start:seg_end]
            exec_num = int(m.group(1))

            # Source: first code <pre> in this segment — skip error-output pres
            # (e.g. "An error was encountered:", "Traceback", ANSI-stripped tracebacks)
            _ERROR_STARTS = (
                'An error was encountered',
                'Traceback (most recent',
                '-----------',
                'Error:',
                'Exception:',
                'WARNING:',
            )
            source = None
            for src_m in re.finditer(r'<pre>(.*?)</pre>', seg, re.DOTALL):
                candidate = _clean_html(src_m.group(1))[:600]
                if (len(candidate) < 10
                        or candidate.startswith('.')
                        or 'color:' in candidate[:50]
                        or any(candidate.startswith(p) for p in _ERROR_STARTS)):
                    continue
                source = candidate
                break
            if not source:
                continue

            # Errors in this segment
            for pattern, err_type in EXCEPTION_PATTERNS:
                em = re.search(pattern, seg, re.IGNORECASE | re.DOTALL)
                if em:
                    clean_err = _clean_html(em.group(0))[:350]
                    if clean_err and len(clean_err) > 15:
                        results.append({
                            'cell_num': exec_num,
                            'source': source,
                            'error_type': err_type,
                            'error_text': clean_err,
                        })
                        break  # one error entry per cell

        if results:
            return results

    # ── Strategy 2 (fallback): position-based correlation ───────────────────
    pre_positions = []
    for pm in re.finditer(r'<pre>(.*?)</pre>', html, re.DOTALL):
        clean = _clean_html(pm.group(1))
        if (clean and len(clean) > 20
                and not clean.startswith('.')
                and not clean.startswith('{')
                and 'color:' not in clean[:50]):
            pre_positions.append({
                'start': pm.start(),
                'source': clean[:600],
                'index': len(pre_positions) + 1,  # 1-based cell number
            })

    seen: set = set()
    for pattern, err_type in EXCEPTION_PATTERNS:
        for em in re.finditer(pattern, html, re.IGNORECASE | re.DOTALL):
            err_pos = em.start()
            err_text = _clean_html(em.group(0))[:350]
            if not err_text or len(err_text) < 15:
                continue

            # Nearest preceding code block
            nearest = None
            for pre in pre_positions:
                if pre['start'] < err_pos:
                    nearest = pre
                else:
                    break

            if nearest:
                key = (nearest['index'], err_type)
                if key not in seen:
                    seen.add(key)
                    results.append({
                        'cell_num': nearest['index'],
                        'source': nearest['source'],
                        'error_type': err_type,
                        'error_text': err_text,
                    })

    return results


def build_notebook_context(html: str) -> dict:
    """
    Full parse of a rendered notebook HTML.
    Returns structured dict for use in the LLM prompt.
    """
    errors = extract_errors(html)
    cell_sources = extract_cell_sources(html)
    failing_cells = extract_failing_cells(html)

    return {
        "errors": errors,
        "cellSources": cell_sources,
        "cellCount": len(cell_sources),
        "failingCells": failing_cells,
        "htmlSize": len(html),
    }
