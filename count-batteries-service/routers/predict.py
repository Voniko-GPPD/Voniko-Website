"""
Prediction router – image upload and AI battery detection.
Auth is handled by the Node.js proxy: user info arrives via request headers.
"""
from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Request, Depends
from sqlalchemy.orm import Session
from typing import Optional
import os

from models.database import get_db, DetectionRecord
from services.ai_engine import ai_engine

router = APIRouter(prefix="/predict", tags=["Prediction"])


def _user_from_headers(request: Request) -> dict:
    return {
        "id": request.headers.get("x-user-id", ""),
        "username": request.headers.get("x-username", "anonymous"),
        "role": request.headers.get("x-user-role", "user"),
    }


@router.post("")
async def predict(
    request: Request,
    file: UploadFile = File(...),
    confidence: float = Form(0.5),
    save_result: bool = Form(True),
    po_number: Optional[str] = Form(None),
    db: Session = Depends(get_db),
):
    """Upload an image and count batteries using YOLO/ONNX AI."""
    po_number = po_number.strip() or None if po_number else None

    if not file.content_type or not file.content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="File must be an image")

    contents = await file.read()

    result = ai_engine.process_image(contents, confidence, po_number=po_number)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])

    user = _user_from_headers(request)
    device_info = request.headers.get("user-agent", "Unknown")

    result_path = None
    record_id = None
    if save_result:
        static_dir = os.getenv("COUNT_BATTERIES_STATIC_DIR", "./static")
        results_dir = os.path.join(static_dir, "results")
        result_path = ai_engine.save_result_image(
            contents, result["detections"], results_dir, po_number=po_number
        )

        record = DetectionRecord(
            user_id=user["id"],
            username=user["username"],
            user_role=user["role"],
            count=result["count"],
            result_image_path=result_path,
            po_number=po_number,
            device_info=device_info[:255] if device_info else None,
        )
        db.add(record)
        db.commit()
        db.refresh(record)
        record_id = record.id

    return {
        "status": "success",
        "count": result["count"],
        "detections": result["detections"],
        "result_image": result["result_image"],
        "result_image_path": result_path if save_result else None,
        "record_id": record_id,
        "po_number": po_number,
    }


@router.post("/quick")
async def predict_quick(
    file: UploadFile = File(...),
    confidence: float = Form(0.5),
):
    """Quick prediction – no auth required, result not saved."""
    if not file.content_type or not file.content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="File must be an image")

    contents = await file.read()
    result = ai_engine.process_image(contents, confidence)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])

    return {
        "status": "success",
        "count": result["count"],
        "result_image": result["result_image"],
    }
