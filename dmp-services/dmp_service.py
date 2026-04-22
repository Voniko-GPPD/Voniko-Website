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
from datetime import date
from io import BytesIO
from pathlib import Path

import pyodbc
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from openpyxl import load_workbook
from typing import Optional

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


DMP_STATION_NAME: str = os.environ.get("DMP_STATION_NAME", "").strip()
VONIKO_SERVER_URL: str = os.environ.get("VONIKO_SERVER_URL", "").rstrip("/")
DMP_STATION_PORT: int = int(os.environ.get("DMP_STATION_PORT", "8766"))
DMP_DATA_DIR: str = os.environ.get("DMP_DATA_DIR", r"C:\DMP\Data")
DMP_TEMPLATES_DIR: str = os.environ.get("DMP_TEMPLATES_DIR", "./dmp_templates")
DM2000_DATA_DIR: str = os.environ.get("DM2000_DATA_DIR", r"D:\DM2000\dmdatabase")
DM2000_TEMPLATES_DIR: str = os.environ.get("DM2000_TEMPLATES_DIR", "./dm2000_templates")
WATCH_INTERVAL_SECONDS: int = 5

_WATCH_LOCK = threading.Lock()
_ACCESS_QUERY_LOCK = threading.Semaphore(5)
_ACCESS_QUERY_TIMEOUT: float = 60.0  # seconds to wait for a DB slot before returning 503
# Limit concurrent DM2000 shadow-copy + query operations to avoid memory/disk exhaustion
# when many requests arrive simultaneously (e.g. when a user rapidly switches archives).
# Each shadow copy duplicates dmdata_ls.mdb on disk and loads its query results into memory,
# so limiting to 2 concurrent operations keeps peak resource usage predictable.
_DM2000_LS_LOCK = threading.Semaphore(2)
_TELEMETRY_CACHE: dict[tuple, tuple[list, float]] = {}
_TELEMETRY_CACHE_LOCK = threading.Lock()
_TELEMETRY_CACHE_TTL: float = 60.0  # seconds
_DM2000_CURVE_CACHE: dict[tuple, tuple[list, float]] = {}
_DM2000_CURVE_CACHE_LOCK = threading.Lock()
_DM2000_CURVE_CACHE_TTL: float = 300.0  # seconds
_DM2000_ARCHIVES_CACHE: dict[str, tuple[list, float]] = {}
_DM2000_ARCHIVES_CACHE_LOCK = threading.Lock()
_DM2000_ARCHIVES_CACHE_TTL: float = 60.0  # seconds
_DM2000_BATTERIES_CACHE: dict[str, tuple[list, float]] = {}
_DM2000_BATTERIES_CACHE_LOCK = threading.Lock()
_DM2000_BATTERIES_CACHE_TTL: float = 60.0  # seconds
_DM2000_CACHE_MAX_ENTRIES: int = 100
_WATCHED_MDB_MTIME: dict[str, float] = {}
_WATCHED_CHANGES: dict[str, float] = {}
_SCHEMA_TABLE_WHITELIST = {
    "para_singl",
    "para_pub",
    "vidata",
    "ls_jb_cs",
    "ls_pam2",
    "ls_vtime",
    "ls_evolt",
    "ls_timev",
}
# Cached result of para_pub schema check (None = not yet determined)
_HAS_PARA_PUB_CDMC: bool | None = None
_HAS_PARA_PUB_CDMC_LOCK = threading.Lock()


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
def shadow_copy_any(mdb_path: str, allowed_dir: str):
    """Generic shadow copy with path restriction under allowed_dir."""
    source = Path(mdb_path).resolve()
    base_dir = Path(allowed_dir).resolve()
    if source.suffix.lower() != ".mdb":
        raise HTTPException(status_code=400, detail="Invalid file type")
    if not source.exists() or not source.is_file():
        raise HTTPException(status_code=404, detail="MDB file not found")
    try:
        source.relative_to(base_dir)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid path") from exc

    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".mdb")
    os.close(tmp_fd)
    try:
        try:
            shutil.copy2(str(source), tmp_path)
        except PermissionError as exc:
            logger.warning("shadow_copy: PermissionError copying %s — file locked by DMP app: %s", source, exc)
            raise HTTPException(
                status_code=503,
                detail=f"MDB file is locked by DMP application, retry later: {exc}",
            ) from exc
        except OSError as exc:
            logger.warning("shadow_copy: OSError copying %s: %s", source, exc)
            raise HTTPException(
                status_code=503,
                detail=f"Cannot read MDB file: {exc}",
            ) from exc

        # Validate: must be at least 32 KB and start with Jet/ACE magic bytes
        copied_size = os.path.getsize(tmp_path)
        if copied_size < 32 * 1024:
            raise HTTPException(
                status_code=422,
                detail=f"File too small ({copied_size} bytes) to be a valid Access database (possible Windows shortcut)",
            )
        with open(tmp_path, "rb") as fh:
            magic = fh.read(4)
        if magic != b"\x00\x01\x00\x00":
            raise HTTPException(
                status_code=422,
                detail="Not a valid Access database header (possible Windows shortcut)",
            )

        yield tmp_path
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


@contextmanager
def shadow_copy(mdb_path: str):
    """Copy live DMP .mdb to temp location before querying to avoid file-lock errors."""
    with shadow_copy_any(mdb_path, DMP_DATA_DIR) as tmp:
        yield tmp


@contextmanager
def shadow_copy_dm2000(mdb_path: str):
    """Copy DM2000 .mdb to a temp file before querying to avoid lock conflicts."""
    with shadow_copy_any(mdb_path, DM2000_DATA_DIR) as tmp:
        yield tmp


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


@contextmanager
def _acquire_query_lock():
    """Acquire the Access DB semaphore with a timeout.

    Raises HTTPException(503) instead of blocking indefinitely when the
    database is saturated with concurrent requests.
    """
    acquired = _ACCESS_QUERY_LOCK.acquire(timeout=_ACCESS_QUERY_TIMEOUT)
    if not acquired:
        raise HTTPException(
            status_code=503,
            detail="Database is busy with too many concurrent requests, please retry shortly",
        )
    try:
        yield
    finally:
        _ACCESS_QUERY_LOCK.release()


def query_mdb(mdb_path: str, sql: str, params: tuple = (), retries: int = 2) -> list[dict]:
    """Run an Access query with bounded concurrency and short HY000 retries.

    Retries are only applied to transient HY000 driver errors using a linear
    backoff of 0.3s * attempt and preserve existing HYC00 inline fallback.
    """
    conn_str = (
        r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
        f"DBQ={mdb_path};"
    )
    for attempt_num in range(1, retries + 2):
        try:
            with _acquire_query_lock():
                with pyodbc.connect(conn_str, timeout=10) as conn:
                    cursor = conn.cursor()
                    try:
                        cursor.execute(sql, params) if params else cursor.execute(sql)
                    except pyodbc.Error as exc:
                        err_str = str(exc)
                        if params and ("HYC00" in err_str or "SQLBindParameter" in err_str or "07002" in err_str):
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
        except pyodbc.Error as exc:
            err_str = str(exc)
            if "HY000" in err_str and attempt_num <= retries:
                wait = 0.3 * attempt_num
                logger.debug("HY000 on attempt %d, retrying in %.1fs: %s", attempt_num, wait, exc)
                time.sleep(wait)
                continue
            raise


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


def get_dm2000_ls_path() -> str:
    return str(Path(DM2000_DATA_DIR).resolve() / "dmdata_ls.mdb")


def get_dm2000_main_path() -> str:
    return str(Path(DM2000_DATA_DIR).resolve() / "DM2000.mdb")


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


class DM2000ReportRequest(BaseModel):
    archname: str
    baty: int = Field(ge=0)
    template_name: str = Field(min_length=6)
    override_archname: Optional[str] = None
    override_start_date: Optional[str] = None
    override_battery_type: Optional[str] = None
    override_batch_name: Optional[str] = None
    override_discharge_condition: Optional[str] = None
    override_manufacturer: Optional[str] = None
    override_made_date: Optional[str] = None
    override_serial_no: Optional[str] = None
    override_remarks: Optional[str] = None


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
    if not _is_valid_template_name(template_name):
        raise HTTPException(status_code=400, detail="Invalid template")

    base = Path(DMP_TEMPLATES_DIR).resolve()
    allowed = {
        f.name for f in base.iterdir()
        if f.is_file() and _is_valid_template_name(f.name)
    } if base.exists() else set()
    if template_name not in allowed:
        raise HTTPException(status_code=404, detail="Template not found")
    result = (base / template_name).resolve()
    try:
        result.relative_to(base)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Template path traversal detected") from exc
    if not result.exists():
        raise HTTPException(status_code=404, detail="Template not found")
    return str(result)


def _read_dmpdata(sql: str, params: tuple = ()) -> list[dict]:
    dmpdata = Path(get_dmpdata_path())
    if not dmpdata.exists():
        raise HTTPException(status_code=404, detail="DMPDATA.mdb not found")
    with shadow_copy(str(dmpdata)) as copied:
        return query_mdb(copied, sql, params)


def _read_dm2000_ls(sql: str, params: tuple = ()) -> list[dict]:
    ls_path = Path(get_dm2000_ls_path())
    if not ls_path.exists():
        raise HTTPException(status_code=404, detail="dmdata_ls.mdb not found")
    acquired = _DM2000_LS_LOCK.acquire(timeout=_ACCESS_QUERY_TIMEOUT)
    if not acquired:
        raise HTTPException(
            status_code=503,
            detail="Database is busy with too many concurrent requests, please retry shortly",
        )
    try:
        with shadow_copy_dm2000(str(ls_path)) as copied:
            return query_mdb(copied, sql, params)
    finally:
        _DM2000_LS_LOCK.release()


def _resolve_dm2000_template_path(template_name: str) -> str:
    if not _is_valid_template_name(template_name):
        raise HTTPException(status_code=400, detail="Invalid template")

    base = Path(DM2000_TEMPLATES_DIR).resolve()
    allowed = {
        f.name for f in base.iterdir()
        if f.is_file() and _is_valid_template_name(f.name)
    } if base.exists() else set()
    if template_name not in allowed:
        raise HTTPException(status_code=404, detail="Template not found")
    result = (base / template_name).resolve()
    try:
        result.relative_to(base)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Template path traversal detected") from exc
    if not result.exists():
        raise HTTPException(status_code=404, detail="Template not found")
    return str(result)


def compute_dm2000_stats(rows: list[dict]) -> dict:
    """Compute DM2000 curve statistics. TIM is already in minutes."""
    def safe_float(v):
        if v is None or v == "--" or v == "":
            return None
        try:
            f = float(v)
            return None if math.isnan(f) else f
        except (TypeError, ValueError):
            return None

    volt_vals = [safe_float(r.get("VOLT") or r.get("volt")) for r in rows]
    volt_vals = [v for v in volt_vals if v is not None]

    tim_vals = [safe_float(r.get("TIM") or r.get("tim")) for r in rows]
    tim_vals = [t for t in tim_vals if t is not None]

    def agg(vals):
        if not vals:
            return {"max": None, "min": None, "avg": None}
        return {
            "max": round(max(vals), 4),
            "min": round(min(vals), 4),
            "avg": round(sum(vals) / len(vals), 4),
        }

    v = agg(volt_vals)
    duration = max(tim_vals) if tim_vals else None
    return {
        "VOLT_MAX": v["max"],
        "VOLT_MIN": v["min"],
        "VOLT_AVG": v["avg"],
        "DURATION_MIN": duration,
    }


def _compute_average_curve(rows: list[dict]) -> list[dict]:
    """Group by TIM and average VOLT values."""
    from collections import defaultdict
    tim_groups: dict[float, list[float]] = defaultdict(list)
    for row in rows:
        tim = row.get("TIM") or row.get("tim")
        volt = row.get("VOLT") or row.get("volt")
        try:
            t = float(tim)
            v = float(volt)
            if not math.isnan(t) and not math.isnan(v):
                tim_groups[t].append(v)
        except (TypeError, ValueError):
            continue
    return [
        {"TIM": t, "VOLT": round(sum(vs) / len(vs), 6)}
        for t, vs in sorted(tim_groups.items())
    ]


def _get_evolt_volt_stats(archname: str, baty: int) -> dict | None:
    """Get VOLT_MAX (OCV), VOLT_MIN (FCV), VOLT_AVG from ls_evolt for a battery.

    Returns a dict with VOLT_MAX, VOLT_MIN, VOLT_AVG, or None if no data found.

    The data is queried ORDER BY date/daytime ASC so the first row is the OCV
    (voltage measured before discharge begins, always the highest value) and
    the last row is the FCV (voltage at the end of discharge, always the lowest).
    Using positional first/last rather than max/min is intentional: OCV and FCV
    are defined as the chronologically first and last measurements, not simply
    the extreme values in the series.
    """
    def safe_float(v):
        if v is None or v in ("--", ""):
            return None
        try:
            f = float(v)
            return None if math.isnan(f) else f
        except (TypeError, ValueError):
            return None

    volt_rows = []
    # Try archname-based schema first
    try:
        rows = _read_dm2000_ls(
            "SELECT volt FROM ls_evolt WHERE archname = ? AND baty = ? ORDER BY date ASC",
            (archname, baty),
        )
        if rows:
            volt_rows = [safe_float(r.get("volt")) for r in rows]
    except pyodbc.Error:
        pass

    if not volt_rows:
        # Try cdid-based schema
        volt_col = f"volt{baty}"
        try:
            rows = _read_dm2000_ls(
                f"SELECT {volt_col} AS volt FROM ls_evolt WHERE cdid = ? ORDER BY daytime ASC",
                (archname,),
            )
            volt_rows = [safe_float(r.get("volt")) for r in rows]
        except pyodbc.Error:
            pass

    volt_vals = [v for v in volt_rows if v is not None]
    if not volt_vals:
        return None

    return {
        "VOLT_MAX": volt_vals[0],   # OCV – first daily measurement (start of discharge)
        "VOLT_MIN": volt_vals[-1],  # FCV – last daily measurement (end of discharge)
        "VOLT_AVG": round(sum(volt_vals) / len(volt_vals), 4),
    }


def _dm2000_get_value(row: dict, *keys):
    for key in keys:
        if key in row and row.get(key) not in (None, ""):
            return row.get(key)
    lowered = {str(k).lower(): v for k, v in row.items()}
    for key in keys:
        value = lowered.get(key.lower())
        if value not in (None, ""):
            return value
    return None


def _is_valid_template_name(name: str) -> bool:
    if not isinstance(name, str):
        return False
    if "/" in name or "\\" in name:
        return False
    if not name.endswith(".xlsx"):
        return False
    stem = name[:-5]
    return bool(stem) and all(ch.isalnum() or ch in "_-" for ch in stem)


def _validate_dm2000_archname(archname: str) -> None:
    if not re.match(r'^[A-Za-z0-9_.\-]+$', archname):
        raise HTTPException(status_code=400, detail="Invalid archname")


def _cache_set_with_cap(cache: dict, key, value) -> None:
    cache[key] = value
    if len(cache) <= _DM2000_CACHE_MAX_ENTRIES:
        return
    oldest_key = min(cache.items(), key=lambda item: item[1][1])[0]
    cache.pop(oldest_key, None)


def _read_dm2000_curve_rows(archname: str, baty: int) -> list[dict]:
    cache_key = (archname, baty)
    with _DM2000_CURVE_CACHE_LOCK:
        cached = _DM2000_CURVE_CACHE.get(cache_key)
        if cached is not None:
            rows, ts = cached
            if time.time() - ts < _DM2000_CURVE_CACHE_TTL:
                return rows

    try:
        rows = _read_dm2000_ls(
            "SELECT TIM, VOLT FROM ls_vtime WHERE archname = ? AND baty = ? ORDER BY TIM ASC",
            (archname, baty),
        )
    except pyodbc.Error:
        if baty <= 0 or baty > 99:
            raise HTTPException(status_code=400, detail="Invalid baty")
        time_col = f"time{baty}"
        try:
            rows = _read_dm2000_ls(
                f"SELECT dy, {time_col} AS TIM FROM ls_vtime WHERE cdid = ? ORDER BY {time_col} ASC",
                (archname,),
            )
        except pyodbc.Error:
            rows = []
        curve = []
        for row in rows:
            tim = row.get("TIM")
            volt = row.get("dy")
            try:
                t = float(tim)
                v = float(volt)
                if not math.isnan(t) and not math.isnan(v):
                    curve.append({"TIM": t, "VOLT": v})
            except (TypeError, ValueError):
                continue
        rows = curve

    with _DM2000_CURVE_CACHE_LOCK:
        _cache_set_with_cap(_DM2000_CURVE_CACHE, cache_key, (rows, time.time()))
    return rows


def _read_dm2000_average_curve_rows(archname: str) -> list[dict]:
    cache_key = ("avg", archname)
    with _DM2000_CURVE_CACHE_LOCK:
        cached = _DM2000_CURVE_CACHE.get(cache_key)
        if cached is not None:
            rows, ts = cached
            if time.time() - ts < _DM2000_CURVE_CACHE_TTL:
                return rows

    try:
        rows = _read_dm2000_ls(
            "SELECT baty, TIM, VOLT FROM ls_vtime WHERE archname = ? ORDER BY baty ASC, TIM ASC",
            (archname,),
        )
        avg_rows = _compute_average_curve(rows)
    except pyodbc.Error:
        rows = _read_dm2000_ls(
            "SELECT dy, time1, time2, time3, time4, time5, time6, time7, time8, time9 FROM ls_vtime WHERE cdid = ?",
            (archname,),
        )
        flattened: list[dict] = []
        for row in rows:
            volt = row.get("dy")
            for idx in range(1, 10):
                tim = row.get(f"time{idx}")
                flattened.append({"TIM": tim, "VOLT": volt})
        avg_rows = _compute_average_curve(flattened)

    with _DM2000_CURVE_CACHE_LOCK:
        _cache_set_with_cap(_DM2000_CURVE_CACHE, cache_key, (avg_rows, time.time()))
    return avg_rows


def _parse_iso_date_param(value: str | None, field_name: str) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}, expected YYYY-MM-DD") from exc


def _query_vidata_by_channel(mdb_path: str, channel: int) -> list[dict]:
    """
    Query vidata with fallback strategy for Access ODBC parameter quirks.
    """
    base_sql = "SELECT baty, TIM, VOLT, Im FROM vidata WHERE baty = {placeholder} ORDER BY TIM ASC"

    try:
        return query_mdb(mdb_path, base_sql.format(placeholder="?"), (channel,))
    except HTTPException:
        raise
    except Exception as exc1:
        logger.debug("_query_vidata attempt1 (int param) failed: %s", exc1)

    try:
        return query_mdb(mdb_path, base_sql.format(placeholder="?"), (str(channel),))
    except HTTPException:
        raise
    except Exception as exc2:
        logger.debug("_query_vidata attempt2 (str param) failed: %s", exc2)

    try:
        return query_mdb(
            mdb_path,
            f"SELECT baty, TIM, VOLT, Im FROM vidata WHERE baty = {int(channel)} ORDER BY TIM ASC",
        )
    except HTTPException:
        raise
    except Exception as exc3:
        logger.debug("_query_vidata attempt3 (inline int) failed: %s", exc3)

    try:
        return query_mdb(
            mdb_path,
            f"SELECT baty, TIM, VOLT, Im FROM vidata WHERE baty = '{int(channel)}' ORDER BY TIM ASC",
        )
    except HTTPException:
        raise
    except Exception as exc4:
        logger.debug("_query_vidata attempt4 (inline str) failed: %s", exc4)
        raise HTTPException(
            status_code=500,
            detail=f"Cannot query vidata for channel {channel}: {exc4}",
        ) from exc4


def _read_telemetry(cdmc: str, channel: int) -> list[dict]:
    cache_key = (cdmc, channel)
    with _TELEMETRY_CACHE_LOCK:
        cached = _TELEMETRY_CACHE.get(cache_key)
        if cached is not None:
            rows, ts = cached
            if time.time() - ts < _TELEMETRY_CACHE_TTL:
                logger.debug("_read_telemetry cache hit: cdmc=%r channel=%d", cdmc, channel)
                return rows

    mdb_path = resolve_data_file(cdmc)
    try:
        with shadow_copy(mdb_path) as copied:
            rows = _query_vidata_by_channel(copied, channel)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("_read_telemetry: unexpected error for cdmc=%r channel=%d", cdmc, channel)
        raise HTTPException(status_code=500, detail=f"Unexpected error reading telemetry: {exc}") from exc
    for row in rows:
        tim = row.get("TIM")
        if tim is not None and tim != "--":
            try:
                row["TIM"] = round(float(tim) / 3600, 6)
            except (TypeError, ValueError):
                pass
    with _TELEMETRY_CACHE_LOCK:
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
    with _acquire_query_lock():
        with pyodbc.connect(conn_str, timeout=10) as conn:
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
    global _HAS_PARA_PUB_CDMC

    # Determine schema once and cache for the lifetime of the process
    with _HAS_PARA_PUB_CDMC_LOCK:
        if _HAS_PARA_PUB_CDMC is None:
            detected = True  # default: assume cdmc column exists
            dmpdata = Path(get_dmpdata_path())
            if dmpdata.exists():
                try:
                    with shadow_copy(str(dmpdata)) as copied:
                        try:
                            para_pub_columns = _get_table_columns(copied, "para_pub")
                            detected = "cdmc" in para_pub_columns
                        except (pyodbc.Error, HTTPException) as exc:
                            logger.debug("Could not inspect para_pub schema, using query fallback: %s", exc)
                except (pyodbc.Error, HTTPException) as exc:
                    logger.debug("Could not create shadow copy for schema inspection: %s", exc)
            _HAS_PARA_PUB_CDMC = detected

    has_para_pub_cdmc = _HAS_PARA_PUB_CDMC

    try:
        if has_para_pub_cdmc:
            rows = _read_dmpdata("SELECT id, cdmc, dcxh, fdrq, fdfs FROM para_pub ORDER BY fdrq DESC")
        else:
            rows = _read_dmpdata("SELECT id, dcxh, fdrq, fdfs FROM para_pub ORDER BY fdrq DESC")
    except pyodbc.Error:
        try:
            rows = _read_dmpdata("SELECT id, dcxh, fdrq, fdfs FROM para_pub ORDER BY fdrq DESC")
        except pyodbc.Error as exc:
            logger.error("get_batches: fallback query also failed: %s", exc)
            raise HTTPException(status_code=500, detail="Database query failed") from exc
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


# ─── DM2000 Historic Database Routes ────────────────────────────────────────

@app.get("/dm2000/archives")
def get_dm2000_archives(
    date_from: str = None,
    date_to: str = None,
    type_filter: str = None,
    name_filter: str = None,
    mfr_filter: str = None,
    serial_filter: str = None,
    limit: int = 500,
):
    table_names_to_try = ["ls_jb_cs", "ls_pam2", "ls_cs", "ls_jbcs", "ls_archive"]
    rows = None
    cache_key = "all_archives"
    with _DM2000_ARCHIVES_CACHE_LOCK:
        cached = _DM2000_ARCHIVES_CACHE.get(cache_key)
        if cached is not None:
            cached_rows, ts = cached
            if time.time() - ts < _DM2000_ARCHIVES_CACHE_TTL:
                rows = cached_rows

    if rows is None:
        for table_name in table_names_to_try:
            try:
                rows = _read_dm2000_ls(f"SELECT * FROM {table_name}")
                break
            except (pyodbc.Error, HTTPException):
                continue
        if rows is None:
            return {"archives": [], "total": 0, "warning": "Archive table not found in dmdata_ls.mdb"}
        with _DM2000_ARCHIVES_CACHE_LOCK:
            _cache_set_with_cap(_DM2000_ARCHIVES_CACHE, cache_key, (rows, time.time()))

    date_from_parsed = _parse_iso_date_param(date_from, "date_from")
    date_to_parsed = _parse_iso_date_param(date_to, "date_to")

    def _to_date_text(value):
        if value and hasattr(value, "strftime"):
            return value.strftime("%Y-%m-%d")
        return str(value)[:10] if value not in (None, "") else ""

    archives = []
    for row in rows:
        item = {
            "archname": _dm2000_get_value(row, "archname", "cdid", "id"),
            "startdate": _to_date_text(_dm2000_get_value(row, "startdate", "fdrq")),
            "dcxh": _dm2000_get_value(row, "dcxh"),
            "name": _dm2000_get_value(row, "name", "dcmc"),
            "fdfs": _dm2000_get_value(row, "fdfs"),
            "duration": _dm2000_get_value(row, "duration", "fdts"),
            "unifrate": _dm2000_get_value(row, "unifrate", "yfws"),
            "manufacturer": _dm2000_get_value(row, "manufacturer", "scdw"),
            "madedate": _to_date_text(_dm2000_get_value(row, "madedate", "scrq")),
            "serialno": _dm2000_get_value(row, "serialno", "dcph"),
            "remarks": _dm2000_get_value(row, "remarks", "bz"),
        }
        archives.append(item)

    def _contains(value, pattern):
        if not pattern:
            return True
        return pattern.lower() in str(value or "").lower()

    filtered = []
    for row in archives:
        row_date = None
        if row["startdate"]:
            try:
                row_date = date.fromisoformat(str(row["startdate"])[:10])
            except ValueError:
                row_date = None
        if date_from_parsed and row_date and row_date < date_from_parsed:
            continue
        if date_to_parsed and row_date and row_date > date_to_parsed:
            continue
        if not _contains(row.get("dcxh"), type_filter):
            continue
        if not _contains(row.get("name"), name_filter):
            continue
        if not _contains(row.get("manufacturer"), mfr_filter):
            continue
        if not _contains(row.get("serialno"), serial_filter):
            continue
        filtered.append(row)

    filtered.sort(key=lambda r: str(r.get("startdate") or ""), reverse=True)
    if limit is not None and limit > 0:
        filtered = filtered[:limit]
    return {"archives": filtered, "total": len(filtered)}


def _derive_dm2000_batteries_from_vtime(archname: str) -> list[dict]:
    """Fallback: derive active battery channels from ls_vtime time1..time9 columns.

    For cdid-based ls_vtime, each row holds one voltage threshold with elapsed
    time per battery in time{n}. A battery channel is considered active when at
    least one of its time{n} values is non-null/non-empty.

    For archname-based ls_vtime (which uses a baty column instead of time1..time9),
    falls back to querying DISTINCT baty values directly.
    """
    select_cols = ", ".join(f"time{i}" for i in range(1, 10))
    try:
        rows = _read_dm2000_ls(
            f"SELECT {select_cols} FROM ls_vtime WHERE cdid = ?",
            (archname,),
        )
    except pyodbc.Error:
        # cdid-based schema failed; try archname-based schema
        try:
            baty_rows = _read_dm2000_ls(
                "SELECT DISTINCT baty FROM ls_vtime WHERE archname = ? ORDER BY baty ASC",
                (archname,),
            )
        except pyodbc.Error:
            return []
        result = []
        for row in baty_rows:
            val = _dm2000_get_value(row, "baty")
            try:
                num = int(float(val))
                if num > 0:
                    result.append({"baty": num})
            except (TypeError, ValueError):
                pass
        return result

    active: set[int] = set()
    for row in rows:
        for i in range(1, 10):
            value = row.get(f"time{i}")
            if value in (None, "", "--"):
                continue
            try:
                num = float(value)
            except (TypeError, ValueError):
                continue
            if math.isnan(num):
                continue
            active.add(i)
    return [{"baty": i} for i in sorted(active)]


@app.get("/dm2000/archives/{archname}/batteries")
def get_dm2000_batteries(archname: str):
    _validate_dm2000_archname(archname)

    with _DM2000_BATTERIES_CACHE_LOCK:
        cached = _DM2000_BATTERIES_CACHE.get(archname)
        if cached is not None:
            rows, ts = cached
            if time.time() - ts < _DM2000_BATTERIES_CACHE_TTL:
                return {"batteries": rows, "archname": archname}

    try:
        rows = _read_dm2000_ls(
            "SELECT * FROM ls_pam2 WHERE archname = ? ORDER BY baty ASC",
            (archname,),
        )
    except pyodbc.Error:
        try:
            rows = _read_dm2000_ls(
                "SELECT * FROM ls_pam2 WHERE cdid = ? ORDER BY gpp ASC",
                (archname,),
            )
        except pyodbc.Error:
            rows = []
    for row in rows:
        if "baty" not in row:
            row["baty"] = _dm2000_get_value(row, "baty", "gpp")

    # If ls_pam2 has no usable rows (e.g. discharge in progress, or pam2 not
    # populated for this cdid), derive the battery list from ls_vtime so the
    # dropdown still shows each individual pin instead of being empty.
    def _baty_int(value):
        try:
            num = int(float(value))
            return num if num > 0 else None
        except (TypeError, ValueError):
            return None

    has_battery = any(_baty_int(row.get("baty")) is not None for row in rows)
    if not has_battery:
        rows = _derive_dm2000_batteries_from_vtime(archname)

    with _DM2000_BATTERIES_CACHE_LOCK:
        _cache_set_with_cap(_DM2000_BATTERIES_CACHE, archname, (rows, time.time()))
    return {"batteries": rows, "archname": archname}


@app.get("/dm2000/archives/{archname}/curve")
def get_dm2000_curve(archname: str, baty: int):
    _validate_dm2000_archname(archname)
    rows = _read_dm2000_curve_rows(archname, baty)
    return {"curve": rows, "archname": archname, "baty": baty, "time_unit": "minutes"}


@app.get("/dm2000/archives/{archname}/average-curve")
def get_dm2000_average_curve(archname: str):
    _validate_dm2000_archname(archname)
    avg = _read_dm2000_average_curve_rows(archname)
    return {"curve": avg, "archname": archname, "baty": "average", "time_unit": "minutes"}


@app.get("/dm2000/archives/{archname}/stats")
def get_dm2000_stats(archname: str, baty: int = 0):
    _validate_dm2000_archname(archname)
    if baty == 0:
        rows = _read_dm2000_average_curve_rows(archname)
    else:
        rows = _read_dm2000_curve_rows(archname, baty)
    stats = compute_dm2000_stats(rows)
    # Override VOLT_MAX/VOLT_MIN/VOLT_AVG with OCV/FCV from daily voltage (ls_evolt).
    # For the cdid-based schema, ls_vtime stores voltage thresholds rather than real
    # measured voltages, so compute_dm2000_stats produces wrong values (always the
    # fixed threshold limits 1.4 / 0.9).  Using ls_evolt gives the true OCV and FCV.
    if baty > 0:
        evolt_stats = _get_evolt_volt_stats(archname, baty)
        if evolt_stats:
            stats.update(evolt_stats)
    return stats


@app.get("/dm2000/archives/{archname}/daily-voltage")
def get_dm2000_daily_voltage(archname: str, baty: int):
    _validate_dm2000_archname(archname)
    try:
        rows = _read_dm2000_ls(
            "SELECT * FROM ls_evolt WHERE archname = ? AND baty = ? ORDER BY date ASC",
            (archname, baty),
        )
        for row in rows:
            d = row.get("date")
            if d and hasattr(d, "strftime"):
                row["date"] = d.strftime("%Y-%m-%d")
        return {"daily_voltage": rows, "archname": archname, "baty": baty}
    except pyodbc.Error:
        if baty <= 0 or baty > 99:
            raise HTTPException(status_code=400, detail="Invalid baty")
        volt_col = f"volt{baty}"
        try:
            rows = _read_dm2000_ls(
                f"SELECT daytime, {volt_col} AS volt FROM ls_evolt WHERE cdid = ? ORDER BY daytime ASC",
                (archname,),
            )
        except pyodbc.Error:
            return {"daily_voltage": [], "archname": archname, "baty": baty}
        normalized = []
        for row in rows:
            d = row.get("daytime")
            v = row.get("volt")
            if d and hasattr(d, "strftime"):
                d = d.strftime("%Y-%m-%d")
            normalized.append({"date": str(d)[:10] if d not in (None, "") else None, "volt": v})
        return {"daily_voltage": normalized, "archname": archname, "baty": baty}


@app.get("/dm2000/archives/{archname}/time-at-voltage")
def get_dm2000_time_at_voltage(archname: str, baty: int):
    _validate_dm2000_archname(archname)
    try:
        rows = _read_dm2000_ls(
            "SELECT * FROM ls_timev WHERE archname = ? AND baty = ?",
            (archname, baty),
        )
        if rows:
            return {"time_at_voltage": rows, "archname": archname, "baty": baty}
    except pyodbc.Error:
        pass

    if baty <= 0 or baty > 99:
        raise HTTPException(status_code=400, detail="Invalid baty")
    tim_col = f"tim_vot{baty}"
    try:
        rows = _read_dm2000_ls(
            f"SELECT sj, {tim_col} AS minutes FROM ls_timev WHERE cdid = ? ORDER BY sj DESC",
            (archname,),
        )
        if rows:
            return {"time_at_voltage": rows, "archname": archname, "baty": baty}
    except pyodbc.Error:
        pass

    # Final fallback: ls_vtime stores the same time-at-voltage data for the cdid-based
    # schema using dy (voltage threshold) and time1..time9 columns (values in minutes).
    time_col = f"time{baty}"
    try:
        rows = _read_dm2000_ls(
            f"SELECT dy AS sj, {time_col} AS minutes FROM ls_vtime WHERE cdid = ? ORDER BY dy DESC",
            (archname,),
        )
        return {"time_at_voltage": rows, "archname": archname, "baty": baty}
    except pyodbc.Error:
        return {"time_at_voltage": [], "archname": archname, "baty": baty}


@app.get("/dm2000/templates")
def get_dm2000_templates():
    templates_dir = Path(DM2000_TEMPLATES_DIR).resolve()
    if not templates_dir.exists():
        return {"templates": []}
    templates = sorted([
        f.name for f in templates_dir.iterdir()
        if f.is_file() and f.suffix.lower() == ".xlsx"
    ])
    return {"templates": templates}


@app.post("/dm2000/report")
def generate_dm2000_report(payload: DM2000ReportRequest):
    _validate_dm2000_archname(payload.archname)

    try:
        archive_rows = _read_dm2000_ls("SELECT * FROM ls_jb_cs WHERE cdid = ?", (payload.archname,))
    except pyodbc.Error:
        archive_rows = []
    if not archive_rows:
        try:
            archive_rows = _read_dm2000_ls("SELECT * FROM ls_jb_cs WHERE archname = ?", (payload.archname,))
        except pyodbc.Error:
            archive_rows = []
    if not archive_rows:
        raise HTTPException(status_code=404, detail="Archive not found")
    archive = archive_rows[0]

    if payload.baty == 0:
        curve_data = _read_dm2000_average_curve_rows(payload.archname)
        baty_label = "Average"
    else:
        curve_data = _read_dm2000_curve_rows(payload.archname, payload.baty)
        baty_label = str(payload.baty)

    def _apply_override(db_val, override_val):
        return override_val if override_val is not None and str(override_val).strip() != "" else db_val

    stats = compute_dm2000_stats(curve_data)
    # Override VOLT_MAX/VOLT_MIN/VOLT_AVG with OCV/FCV from ls_evolt (daily voltage).
    # For the cdid-based schema the curve VOLT values are fixed voltage thresholds
    # rather than real measurements, so ls_evolt gives the true OCV and FCV.
    if payload.baty > 0:
        evolt_stats = _get_evolt_volt_stats(payload.archname, payload.baty)
        if evolt_stats:
            stats.update(evolt_stats)
    context = {
        "ARCHNAME": _apply_override(_dm2000_get_value(archive, "archname", "cdid", "id"), payload.override_archname),
        "START_DATE": _apply_override(str(_dm2000_get_value(archive, "startdate", "fdrq", "fdkssj", "qyrq", "fdrq") or ""), payload.override_start_date),
        "BATTERY_TYPE": _apply_override(_dm2000_get_value(archive, "dcxh"), payload.override_battery_type),
        "BATCH_NAME": _apply_override(_dm2000_get_value(archive, "name", "dcmc"), payload.override_batch_name),
        "DISCHARGE_CONDITION": _apply_override(_dm2000_get_value(archive, "fdfs"), payload.override_discharge_condition),
        "DURATION": _dm2000_get_value(archive, "duration", "fdts"),
        "UNIFORMITY_RATE": _dm2000_get_value(archive, "unifrate", "yfws"),
        "MANUFACTURER": _apply_override(_dm2000_get_value(archive, "manufacturer", "scdw"), payload.override_manufacturer),
        "MADE_DATE": _apply_override(str(_dm2000_get_value(archive, "madedate", "scrq") or ""), payload.override_made_date),
        "SERIAL_NO": _apply_override(_dm2000_get_value(archive, "serialno", "dcph"), payload.override_serial_no),
        "REMARKS": _apply_override(_dm2000_get_value(archive, "remarks", "bz"), payload.override_remarks),
        "BATTERY_NO": baty_label,
        **stats,
        "HISTORY_DATA": curve_data,
    }

    template_path = _resolve_dm2000_template_path(payload.template_name)
    report_bytes = render_excel_template(template_path, context)
    filename = f"dm2000_report_{payload.archname}_{baty_label}.xlsx"
    return StreamingResponse(
        BytesIO(report_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
