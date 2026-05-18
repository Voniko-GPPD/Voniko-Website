"""
History router – view detection history, stats and export to Excel.
Role-based access is enforced using the x-user-role header from the proxy.
"""
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from sqlalchemy.orm import Session
from sqlalchemy import desc, func
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime, date
import pandas as pd
import os
from pathlib import Path

from models.database import get_db, DetectionRecord

router = APIRouter(prefix="/history", tags=["History"])


class DetectionResponse(BaseModel):
    id: int
    count: int
    result_image_path: Optional[str] = None
    po_number: Optional[str] = None
    device_info: Optional[str] = None
    created_at: datetime
    user_id: Optional[str] = None
    username: Optional[str] = None
    user_role: Optional[str] = None

    class Config:
        from_attributes = True


class HistoryStats(BaseModel):
    total_detections: int
    total_batteries: int
    today_detections: int
    today_batteries: int


class DeleteBatchRequest(BaseModel):
    record_ids: List[int]


def _user_from_headers(request: Request) -> dict:
    return {
        "id": request.headers.get("x-user-id", ""),
        "username": request.headers.get("x-username", "anonymous"),
        "role": request.headers.get("x-user-role", "user"),
    }


@router.get("")
async def get_history(
    request: Request,
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    username: Optional[str] = None,
    po_number: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """
    Get detection history.
    - Admin sees all records (optionally filtered by username).
    - Users see only their own records.
    Returns X-Total-Count header for pagination.
    """
    user = _user_from_headers(request)
    query = db.query(DetectionRecord)

    if user["role"] != "admin":
        query = query.filter(DetectionRecord.user_id == user["id"])
    elif username:
        query = query.filter(DetectionRecord.username == username)

    if date_from:
        query = query.filter(func.date(DetectionRecord.created_at) >= date_from)
    if date_to:
        query = query.filter(func.date(DetectionRecord.created_at) <= date_to)
    if po_number:
        query = query.filter(DetectionRecord.po_number.ilike(f"%{po_number}%"))

    total_count = query.count()
    records = (
        query.order_by(desc(DetectionRecord.created_at))
        .offset(skip)
        .limit(limit)
        .all()
    )

    result = [DetectionResponse.model_validate(r).model_dump(mode="json") for r in records]

    return JSONResponse(
        content=result,
        headers={
            "X-Total-Count": str(total_count),
            "Access-Control-Expose-Headers": "X-Total-Count",
        },
    )


@router.get("/stats", response_model=HistoryStats)
async def get_stats(
    request: Request,
    db: Session = Depends(get_db),
):
    """Get statistics summary."""
    user = _user_from_headers(request)
    query = db.query(DetectionRecord)
    if user["role"] != "admin":
        query = query.filter(DetectionRecord.user_id == user["id"])

    today_str = date.today().isoformat()
    total_detections = query.count()
    total_batteries = db.query(func.sum(DetectionRecord.count)).filter(
        DetectionRecord.id.in_([r.id for r in query.all()])
    ).scalar() or 0

    today_query = query.filter(func.date(DetectionRecord.created_at) == today_str)
    today_detections = today_query.count()
    today_batteries = sum(r.count for r in today_query.all())

    return HistoryStats(
        total_detections=total_detections,
        total_batteries=int(total_batteries),
        today_detections=today_detections,
        today_batteries=today_batteries,
    )


@router.get("/export/excel")
async def export_excel(
    request: Request,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    username: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """Export detection history to Excel."""
    user = _user_from_headers(request)
    query = db.query(DetectionRecord)

    if user["role"] != "admin":
        query = query.filter(DetectionRecord.user_id == user["id"])
    elif username:
        query = query.filter(DetectionRecord.username == username)

    if date_from:
        query = query.filter(func.date(DetectionRecord.created_at) >= date_from)
    if date_to:
        query = query.filter(func.date(DetectionRecord.created_at) <= date_to)

    records = query.order_by(desc(DetectionRecord.created_at)).all()

    data = [
        {
            "ID": r.id,
            "Date/Time": r.created_at.strftime("%Y-%m-%d %H:%M:%S") if r.created_at else "",
            "Battery Count": r.count,
            "PO Number": r.po_number or "",
            "User": r.username or "Anonymous",
            "Device": (r.device_info or "")[:80],
        }
        for r in records
    ]

    df = pd.DataFrame(data)

    static_dir = os.getenv("COUNT_BATTERIES_STATIC_DIR", "./static")
    export_dir = Path(static_dir) / "exports"
    export_dir.mkdir(parents=True, exist_ok=True)

    filename = f"battery_count_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    filepath = export_dir / filename
    df.to_excel(filepath, index=False, engine="openpyxl")

    return FileResponse(
        path=str(filepath),
        filename=filename,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@router.get("/{record_id}", response_model=DetectionResponse)
async def get_record(
    record_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    """Get a specific detection record."""
    user = _user_from_headers(request)
    record = db.query(DetectionRecord).filter(DetectionRecord.id == record_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Record not found")
    if user["role"] != "admin" and record.user_id != user["id"]:
        raise HTTPException(status_code=403, detail="Access denied")
    return record


@router.delete("/batch")
async def delete_batch(
    body: DeleteBatchRequest,
    request: Request,
    db: Session = Depends(get_db),
):
    """Delete multiple detection records."""
    user = _user_from_headers(request)
    records = (
        db.query(DetectionRecord)
        .filter(DetectionRecord.id.in_(body.record_ids))
        .all()
    )
    if not records:
        raise HTTPException(status_code=404, detail="No records found")

    if user["role"] != "admin":
        for r in records:
            if r.user_id != user["id"]:
                raise HTTPException(status_code=403, detail="You can only delete your own records")

    deleted = 0
    for r in records:
        if r.result_image_path and os.path.exists(r.result_image_path):
            try:
                os.remove(r.result_image_path)
            except OSError:
                pass
        db.delete(r)
        deleted += 1

    db.commit()
    return {"message": f"Deleted {deleted} records"}


@router.delete("/{record_id}")
async def delete_record(
    record_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    """Delete a single detection record."""
    user = _user_from_headers(request)
    record = db.query(DetectionRecord).filter(DetectionRecord.id == record_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Record not found")
    if user["role"] != "admin" and record.user_id != user["id"]:
        raise HTTPException(status_code=403, detail="Access denied")

    if record.result_image_path and os.path.exists(record.result_image_path):
        try:
            os.remove(record.result_image_path)
        except OSError:
            pass

    db.delete(record)
    db.commit()
    return {"message": "Record deleted"}
