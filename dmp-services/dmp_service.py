import logging
import math
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

logger = logging.getLogger(__name__)


DMP_STATION_NAME: str = os.environ.get("DMP_STATION_NAME", "").strip()
VONIKO_SERVER_URL: str = os.environ.get("VONIKO_SERVER_URL", "").rstrip("/")
DMP_STATION_PORT: int = int(os.environ.get("DMP_STATION_PORT", "8766"))
DMP_DATA_DIR: str = os.environ.get("DMP_DATA_DIR", r"C:\DMP\Data")
DMP_TEMPLATES_DIR: str = os.environ.get("DMP_TEMPLATES_DIR", "./dmp_templates")
WATCH_INTERVAL_SECONDS: int = 5

_WATCH_LOCK = threading.Lock()
_ACCESS_QUERY_LOCK = threading.Lock()
_TELEMETRY_CACHE: dict[tuple, tuple[list, float]] = {}
_TELEMETRY_CACHE_TTL: float = 60.0  # seconds
_WATCHED_MDB_MTIME: dict[str, float] = {}
_WATCHED_CHANGES: dict[str, float] = {}
_SCHEMA_TABLE_WHITELIST = {"para_singl", "para_pub", "vidata"}


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
            deleted_stems = set(_WATCHED_MDB_MTIME.keys()) - set(current.keys())
            for stem in deleted_stems:
                _WATCHED_CHANGES.pop(stem, None)
                _WATCHED_MDB_MTIME.pop(stem, None)
            for stem, mtime in current.items():
                previous = _WATCHED_MDB_MTIME.get(stem)
                if previous is None or mtime > previous:
                    _WATCHED_CHANGES[stem] = now
                _WATCHED_MDB_MTIME[stem] = mtime
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


def _inline_params(sql: str, params: tuple) -> str:
    """Inline params into SQL for Access ODBC drivers that don't support SQLBindParameter."""
    parts = sql.split("?")
    if len(parts) != len(params) + 1:
        raise ValueError("Parameter count mismatch")
    result = parts[0]
    for i, param in enumerate(params):
        if param is None:
            result += "NULL"
        elif isinstance(param, (int, float)):
            result += str(param)
        else:
            value = str(param)
            # Permit only a conservative character set used by batch/cdmc identifiers
            # and date-like strings when fallback inlining is required by Access ODBC.
            # Includes () , + / # @ which appear in cdmc values and model names.
            if not re.fullmatch(r"[A-Za-z0-9 _:.(),%+/#@-]+", value):
                raise ValueError("Unsafe string parameter for inline SQL fallback")
            escaped = value.replace("'", "''")
            result += f"'{escaped}'"
        result += parts[i + 1]
    return result


def query_mdb(mdb_path: str, sql: str, params: tuple = ()) -> list[dict]:
    with _ACCESS_QUERY_LOCK:
        conn_str = (
            r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
            f"DBQ={mdb_path};"
        )
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, params) if params else cursor.execute(sql)
            except pyodbc.Error as exc:
                err_str = str(exc)
                if params and ("HYC00" in err_str or "SQLBindParameter" in err_str or "07002" in err_str):
                    # MS Access ODBC driver doesn't support bound string parameters;
                    # (errors: HYC00, SQLBindParameter, or sometimes 07002)
                    # fall back to safely inlined parameters on a fresh cursor — the
                    # original cursor's statement handle is invalid after HYC00 and
                    # cannot be reused for another execute() call.
                    try:
                        cursor = conn.cursor()
                        cursor.execute(_inline_params(sql, params))
                    except (pyodbc.Error, ValueError) as inline_exc:
                        logger.warning(
                            "Inline parameter fallback failed (%s); re-raising original execute error (%s)",
                            inline_exc,
                            exc,
                        )
                        raise exc from inline_exc
                else:
                    raise
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
        if value == "--" or value == "" or value is None:
            return None
        try:
            f = float(value)
            return None if math.isnan(f) else f
        except (TypeError, ValueError):
            return None

    def get_value(row: dict, *keys):
        for key in keys:
            if key in row and row.get(key) is not None:
                v = row.get(key)
                if v == "--" or v == "":
                    continue
                return v
        return None

    volt_vals = [safe_float(get_value(r, "VOLT", "volt", "Volt")) for r in rows]
    volt_vals = [v for v in volt_vals if v is not None]
    im_vals_all = [safe_float(get_value(r, "Im", "IM", "im")) for r in rows]
    im_vals_all = [i for i in im_vals_all if i is not None]
    im_vals_active = [i for i in im_vals_all if i > 0]

    def agg(vals):
        if not vals:
            return {"max": None, "min": None, "avg": None}
        return {
            "max": round(max(vals), 4),
            "min": round(min(vals), 4),
            "avg": round(sum(vals) / len(vals), 4),
        }

    v = agg(volt_vals)
    i_all = agg(im_vals_all)
    i_active = agg(im_vals_active)
    return {
        "VOLT_MAX": v["max"],
        "VOLT_MIN": v["min"],
        "VOLT_AVG": v["avg"],
        "IM_MAX": i_all["max"],
        "IM_MIN": i_all["min"],
        "IM_AVG": i_active["avg"],
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


def _query_vidata_by_channel(mdb_path: str, channel: int) -> list[dict]:
    """
    Query vidata with fallback strategy for Access ODBC parameter quirks.
    """
    base_sql = "SELECT baty, TIM, VOLT, Im FROM vidata WHERE baty = {placeholder} ORDER BY TIM ASC"

    try:
        return query_mdb(mdb_path, base_sql.format(placeholder="?"), (channel,))
    except Exception as exc1:
        logger.debug("_query_vidata attempt1 (int param) failed: %s", exc1)

    try:
        return query_mdb(mdb_path, base_sql.format(placeholder="?"), (str(channel),))
    except Exception as exc2:
        logger.debug("_query_vidata attempt2 (str param) failed: %s", exc2)

    try:
        return query_mdb(
            mdb_path,
            f"SELECT baty, TIM, VOLT, Im FROM vidata WHERE baty = {int(channel)} ORDER BY TIM ASC",
        )
    except Exception as exc3:
        logger.debug("_query_vidata attempt3 (inline int) failed: %s", exc3)

    try:
        return query_mdb(
            mdb_path,
            f"SELECT baty, TIM, VOLT, Im FROM vidata WHERE baty = '{int(channel)}' ORDER BY TIM ASC",
        )
    except Exception as exc4:
        logger.debug("_query_vidata attempt4 (inline str) failed: %s", exc4)
        raise HTTPException(
            status_code=500,
            detail=f"Cannot query vidata for channel {channel}: {exc4}",
        ) from exc4


def _read_telemetry(cdmc: str, channel: int) -> list[dict]:
    cache_key = (cdmc, channel)
    cached = _TELEMETRY_CACHE.get(cache_key)
    if cached is not None:
        rows, ts = cached
        if time.time() - ts < _TELEMETRY_CACHE_TTL:
            logger.debug("_read_telemetry cache hit: cdmc=%r channel=%d", cdmc, channel)
            return rows

    mdb_path = resolve_data_file(cdmc)
    with shadow_copy(mdb_path) as copied:
        rows = _query_vidata_by_channel(copied, channel)
    for row in rows:
        tim = row.get("TIM")
        if tim is not None and tim != "--":
            try:
                row["TIM"] = round(float(tim) / 3600, 6)
            except (TypeError, ValueError):
                pass
    _TELEMETRY_CACHE[cache_key] = (rows, time.time())
    return rows


def _get_table_columns(mdb_path: str, table_name: str) -> set[str]:
    if table_name.lower() not in _SCHEMA_TABLE_WHITELIST:
        raise HTTPException(
            status_code=400,
            detail=f"Table '{table_name}' is not whitelisted for schema lookup",
        )
    conn_str = (
        r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
        f"DBQ={mdb_path};"
    )
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        return {
            str(row.column_name).lower()
            for row in cursor.columns(table=table_name)
            if getattr(row, "column_name", None)
        }


@app.get("/")
def health_check():
    return {"status": "ok", "service": "DMP Battery Data Bridge"}


@app.get("/batches")
def get_batches():
    has_para_pub_cdmc = True
    dmpdata = Path(get_dmpdata_path())
    if dmpdata.exists():
        try:
            with shadow_copy(str(dmpdata)) as copied:
                try:
                    para_pub_columns = _get_table_columns(copied, "para_pub")
                    has_para_pub_cdmc = "cdmc" in para_pub_columns
                except (pyodbc.Error, HTTPException) as exc:
                    logger.debug("Could not inspect para_pub schema for get_batches, using query fallback: %s", exc)
        except (pyodbc.Error, HTTPException) as exc:
            logger.debug("Could not create shadow copy for get_batches schema inspection: %s", exc)
    try:
        if has_para_pub_cdmc:
            rows = _read_dmpdata("SELECT id, cdmc, dcxh, fdrq, fdfs FROM para_pub ORDER BY fdrq DESC")
        else:
            rows = _read_dmpdata("SELECT id, dcxh, fdrq, fdfs FROM para_pub ORDER BY fdrq DESC")
    except pyodbc.Error:
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
    """
    Get the channel list for a batch.

    Access schema:
    - para_pub.id = batch_id (Archive ID)
    - para_pub.cdmc = session .mdb file name
    - para_singl.sid = para_pub.id (JOIN key, not para_singl.id)
    - para_singl.baty = channel number
    - para_singl.cdmc = session .mdb file name (can be NULL, fallback to para_pub.cdmc)
    """
    cdmc_value: str | None = None
    try:
        pub_rows = _read_dmpdata("SELECT cdmc FROM para_pub WHERE id = ?", (batch_id,))
        if pub_rows and pub_rows[0].get("cdmc") is not None:
            cdmc_value = str(pub_rows[0]["cdmc"])
    except Exception as exc:
        logger.debug("get_channels: could not read para_pub cdmc for %r: %s", batch_id, exc)

    rows: list[dict] = []
    last_error: Exception | None = None
    try:
        rows = _read_dmpdata(
            "SELECT baty, cdmc FROM para_singl WHERE sid = ?",
            (batch_id,),
        )
    except Exception as exc:
        logger.debug("get_channels: query with cdmc failed for %r: %s", batch_id, exc)
        try:
            rows = _read_dmpdata(
                "SELECT baty FROM para_singl WHERE sid = ?",
                (batch_id,),
            )
        except Exception as exc2:
            logger.debug("get_channels: fallback query failed for %r: %s", batch_id, exc2)
            last_error = exc2

    if not rows and last_error is not None:
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(last_error)}")

    for row in rows:
        if not row.get("cdmc") and cdmc_value:
            row["cdmc"] = cdmc_value

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
