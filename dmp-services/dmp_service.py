import os
import re
import shutil
import socket
import tempfile
import threading
import time
from contextlib import asynccontextmanager, contextmanager
from io import BytesIO
from pathlib import Path

import pyodbc
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from openpyxl import load_workbook
from pydantic import BaseModel


DMP_STATION_NAME: str = os.environ.get("DMP_STATION_NAME", "").strip()
VONIKO_SERVER_URL: str = os.environ.get("VONIKO_SERVER_URL", "").rstrip("/")
DMP_STATION_PORT: int = int(os.environ.get("DMP_STATION_PORT", "8766"))
DMP_DATA_DIR: str = os.environ.get("DMP_DATA_DIR", r"C:\DMP\Data")
DMP_TEMPLATES_DIR: str = os.environ.get("DMP_TEMPLATES_DIR", "./dmp_templates")
WATCH_INTERVAL_SECONDS: int = 5

_WATCH_LOCK = threading.Lock()
_WATCHED_MDB_MTIME: dict[str, float] = {}
_WATCHED_CHANGES: dict[str, float] = {}


def _get_local_ip() -> str:
    try:
        host = VONIKO_SERVER_URL.split("://")[-1].split(":")[0].split("/")[0]
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect((host, 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def _registration_loop() -> None:
    """Background thread: register/heartbeat to Voniko server every 30s."""
    import requests as _req
    while True:
        try:
            ip = _get_local_ip()
            url = f"http://{ip}:{DMP_STATION_PORT}"
            _req.post(
                f"{VONIKO_SERVER_URL}/api/dmp/register",
                json={"name": DMP_STATION_NAME, "url": url},
                timeout=5,
            )
        except Exception:
            pass
        time.sleep(30)


def _scan_dynamic_mdb_files() -> dict[str, float]:
    data_dir = Path(DMP_DATA_DIR).resolve()
    if not data_dir.exists():
        return {}

    result: dict[str, float] = {}
    for entry in data_dir.glob("*.mdb"):
        if not entry.is_file():
            continue
        if entry.name.lower() == "dmpdata.mdb":
            continue
        try:
            result[entry.stem] = entry.stat().st_mtime
        except OSError:
            continue
    return result


def _watch_dmp_changes_loop() -> None:
    with _WATCH_LOCK:
        _WATCHED_MDB_MTIME.update(_scan_dynamic_mdb_files())

    while True:
        current = _scan_dynamic_mdb_files()
        now = time.time()
        with _WATCH_LOCK:
            for stem, mtime in current.items():
                previous = _WATCHED_MDB_MTIME.get(stem)
                if previous is None or mtime > previous:
                    _WATCHED_CHANGES[stem] = now
            _WATCHED_MDB_MTIME.clear()
            _WATCHED_MDB_MTIME.update(current)
        time.sleep(WATCH_INTERVAL_SECONDS)


@asynccontextmanager
async def _lifespan(application):
    watcher_thread = threading.Thread(target=_watch_dmp_changes_loop, daemon=True)
    watcher_thread.start()
    if VONIKO_SERVER_URL and DMP_STATION_NAME:
        t = threading.Thread(target=_registration_loop, daemon=True)
        t.start()
    yield


app = FastAPI(title="DMP Battery Data Bridge", version="1.0.0", lifespan=_lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


@contextmanager
def shadow_copy(mdb_path: str):
    """Copy live .mdb to temp location before querying to avoid file-lock errors."""
    source = Path(mdb_path).resolve()
    base_dir = Path(DMP_DATA_DIR).resolve()
    if source.suffix.lower() != ".mdb":
        raise HTTPException(status_code=400, detail="Invalid file type")
    if not source.exists() or not source.is_file():
        raise HTTPException(status_code=404, detail="MDB file not found")
    if not str(source).startswith(str(base_dir)):
        dmpdata_path = Path(get_dmpdata_path()).resolve()
        if source != dmpdata_path:
            raise HTTPException(status_code=400, detail="Invalid path")

    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".mdb")
    os.close(tmp_fd)
    try:
        shutil.copy2(str(source), tmp_path)
        yield tmp_path
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def query_mdb(mdb_path: str, sql: str, params: tuple = ()) -> list[dict]:
    conn_str = (
        r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
        f"DBQ={mdb_path};"
    )
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        columns = [col[0] for col in cursor.description] if cursor.description else []
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def resolve_data_file(filename: str) -> str:
    """Resolve a filename to absolute path within DMP_DATA_DIR. Raises HTTPException on traversal."""
    parsed = Path(filename)
    if parsed.parent != Path("."):
        raise HTTPException(status_code=400, detail="Invalid filename")
    stem = parsed.stem
    if not re.match(r'^[A-Za-z0-9_-]+$', stem):
        raise HTTPException(status_code=400, detail="Invalid filename")
    result = Path(DMP_DATA_DIR).resolve() / (stem + ".mdb")
    if not str(result).startswith(str(Path(DMP_DATA_DIR).resolve())):
        raise HTTPException(status_code=400, detail="Invalid path")
    if not result.exists():
        raise HTTPException(status_code=404, detail="Data file not found")
    return str(result)


def get_dmpdata_path() -> str:
    return str(Path(DMP_DATA_DIR).resolve() / "DMPDATA.mdb")


def compute_stats(rows: list[dict]) -> dict:
    def safe_float(value):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def get_value(row: dict, *keys):
        for key in keys:
            if key in row and row.get(key) is not None:
                return row.get(key)
        return None

    volt_vals = [safe_float(get_value(r, "VOLT", "volt", "Volt")) for r in rows]
    volt_vals = [v for v in volt_vals if v is not None]
    im_vals = [safe_float(get_value(r, "Im", "IM", "im")) for r in rows]
    im_vals = [i for i in im_vals if i is not None]

    def agg(vals):
        if not vals:
            return {"max": None, "min": None, "avg": None}
        return {
            "max": round(max(vals), 4),
            "min": round(min(vals), 4),
            "avg": round(sum(vals) / len(vals), 4),
        }

    v, i = agg(volt_vals), agg(im_vals)
    return {
        "VOLT_MAX": v["max"],
        "VOLT_MIN": v["min"],
        "VOLT_AVG": v["avg"],
        "IM_MAX": i["max"],
        "IM_MIN": i["min"],
        "IM_AVG": i["avg"],
    }


class ReportRequest(BaseModel):
    batch_id: str
    cdmc: str
    channel: int
    template_name: str


def render_excel_template(template_path: str, context: dict) -> bytes:
    wb = load_workbook(template_path)
    for ws in wb.worksheets:
        _process_worksheet(ws, context)
    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()


def _interpolate_cell(value, ctx: dict):
    if not isinstance(value, str):
        return value
    m = re.fullmatch(r'\{\{(\w+)\}\}', value.strip())
    if m:
        key = m.group(1)
        return ctx.get(key, value)

    def replacer(match):
        key = match.group(1)
        v = ctx.get(key, match.group(0))
        return str(v) if v is not None else ''

    return re.sub(r'\{\{(\w+)\}\}', replacer, value)


def _process_worksheet(ws, ctx: dict):
    history_data = ctx.get("HISTORY_DATA", [])

    rows_to_expand = []
    for row in ws.iter_rows():
        for cell in row:
            if isinstance(cell.value, str) and '{{#HISTORY_DATA}}' in cell.value:
                rows_to_expand.append(cell.row)
                break

    for template_row_idx in sorted(rows_to_expand, reverse=True):
        template_row = list(ws.iter_rows(min_row=template_row_idx, max_row=template_row_idx))[0]

        if history_data:
            if len(history_data) > 1:
                ws.insert_rows(template_row_idx + 1, len(history_data) - 1)

            for i, item in enumerate(history_data):
                target_row = template_row_idx + i
                for j, tmpl_cell in enumerate(template_row):
                    target_cell = ws.cell(row=target_row, column=tmpl_cell.column)
                    if i > 0:
                        target_cell._style = tmpl_cell._style
                    raw = tmpl_cell.value
                    if isinstance(raw, str):
                        raw = raw.replace('{{#HISTORY_DATA}}', '').strip()
                    target_cell.value = _interpolate_cell(raw, item) if raw else raw
        else:
            for cell in template_row:
                cell.value = None

    for row in ws.iter_rows():
        for cell in row:
            if isinstance(cell.value, str) and '{{' in cell.value and '{{#HISTORY_DATA}}' not in cell.value:
                cell.value = _interpolate_cell(cell.value, ctx)


def _resolve_template_path(template_name: str) -> str:
    parsed = Path(template_name)
    if parsed.parent != Path(".") or parsed.suffix.lower() != ".xlsx":
        raise HTTPException(status_code=400, detail="Invalid template")
    if not re.match(r'^[A-Za-z0-9_-]+$', parsed.stem):
        raise HTTPException(status_code=400, detail="Invalid template")

    result = Path(DMP_TEMPLATES_DIR).resolve() / parsed.name
    if not str(result).startswith(str(Path(DMP_TEMPLATES_DIR).resolve())):
        raise HTTPException(status_code=400, detail="Invalid path")
    if not result.exists():
        raise HTTPException(status_code=404, detail="Template not found")
    return str(result)


def _read_dmpdata(sql: str, params: tuple = ()) -> list[dict]:
    dmpdata = Path(get_dmpdata_path())
    if not dmpdata.exists():
        raise HTTPException(status_code=404, detail="DMPDATA.mdb not found")
    with shadow_copy(str(dmpdata)) as copied:
        return query_mdb(copied, sql, params)


def _read_telemetry(cdmc: str, channel: int) -> list[dict]:
    mdb_path = resolve_data_file(cdmc)
    with shadow_copy(mdb_path) as copied:
        return query_mdb(
            copied,
            "SELECT baty, TIM, VOLT, Im FROM vidata WHERE baty = ? ORDER BY TIM ASC",
            (channel,),
        )


@app.get("/")
def health_check():
    return {"status": "ok", "service": "DMP Battery Data Bridge"}


@app.get("/batches")
def get_batches():
    rows = _read_dmpdata("SELECT id, dcxh, fdrq, fdfs FROM para_pub ORDER BY fdrq DESC")
    for row in rows:
        fdrq = row.get("fdrq")
        if fdrq is None:
            continue
        if hasattr(fdrq, "strftime"):
            row["fdrq"] = fdrq.strftime("%Y-%m-%d")
        else:
            row["fdrq"] = str(fdrq)[:10]
    return {"batches": rows}


@app.get("/batches/{batch_id}/channels")
def get_channels(batch_id: str):
    rows = _read_dmpdata("SELECT baty, cdmc FROM para_singl WHERE id=?", (batch_id,))
    return {"channels": rows}


@app.get("/changes")
def get_changes(since: float = 0):
    with _WATCH_LOCK:
        changed = [
            (stem, changed_at)
            for stem, changed_at in _WATCHED_CHANGES.items()
            if changed_at > since
        ]
    changed.sort(key=lambda item: item[1])
    return {"changes": [stem for stem, _ in changed], "timestamp": time.time()}


@app.get("/telemetry")
def get_telemetry(cdmc: str, channel: int):
    return {"telemetry": _read_telemetry(cdmc, channel)}


@app.get("/stats")
def get_stats(cdmc: str, channel: int):
    rows = _read_telemetry(cdmc, channel)
    return compute_stats(rows)


@app.get("/templates")
def get_templates():
    templates_dir = Path(DMP_TEMPLATES_DIR).resolve()
    if not templates_dir.exists():
        return {"templates": []}
    templates = sorted([f.name for f in templates_dir.iterdir() if f.is_file() and f.suffix.lower() == ".xlsx"])
    return {"templates": templates}


@app.post("/report")
def generate_report(payload: ReportRequest):
    batch_rows = _read_dmpdata(
        "SELECT id, dcxh, fdrq, fdfs FROM para_pub WHERE id=?",
        (payload.batch_id,),
    )
    if not batch_rows:
        raise HTTPException(status_code=404, detail="Batch not found")

    telemetry = _read_telemetry(payload.cdmc, payload.channel)
    stats = compute_stats(telemetry)
    batch = batch_rows[0]

    context = {
        "BATCH_ID": batch.get("id"),
        "MODEL": batch.get("dcxh"),
        "DATE": str(batch.get("fdrq")),
        "DISCHARGE_PATTERN": batch.get("fdfs"),
        "CHANNEL": payload.channel,
        **stats,
        "HISTORY_DATA": telemetry,
    }

    template_path = _resolve_template_path(payload.template_name)
    report_bytes = render_excel_template(template_path, context)
    filename = f"dmp_report_{payload.batch_id}_{payload.channel}.xlsx"

    return StreamingResponse(
        BytesIO(report_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
