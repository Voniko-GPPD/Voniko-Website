"""
Count Batteries Service – FastAPI micro-service for battery AI counting.

Authentication is handled by the Voniko-Website Node.js backend:
  - Every request arrives with x-user-id / x-username / x-user-role headers.
  - This service trusts those headers (no independent JWT auth).

Run:
    uvicorn main:app --host 0.0.0.0 --port 8001 --reload
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
import os
from pathlib import Path

from models.database import init_db
from routers import predict, history
from services.ai_engine import ai_engine

STATIC_DIR = Path(os.getenv("COUNT_BATTERIES_STATIC_DIR", "./static"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting Count Batteries Service...")
    init_db()
    (STATIC_DIR / "results").mkdir(parents=True, exist_ok=True)
    (STATIC_DIR / "exports").mkdir(parents=True, exist_ok=True)
    print("Count Batteries Service ready!")
    yield
    print("Shutting down Count Batteries Service.")


app = FastAPI(
    title="Count Batteries Service",
    description="AI battery counting micro-service (proxy-auth via Voniko-Website backend)",
    version="1.0.0",
    lifespan=lifespan,
)

# Only allow requests from the Node.js backend and local dev server
_allowed_origins = os.getenv(
    "COUNT_BATTERIES_CORS_ORIGINS",
    "http://localhost:3001,http://localhost:5173,http://127.0.0.1:3001",
).split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in _allowed_origins],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve result/export images
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

app.include_router(predict.router)
app.include_router(history.router)


@app.get("/health")
async def health():
    model_loaded = ai_engine.is_model_loaded
    return {
        "status": "healthy",
        "service": "Count Batteries Service",
        "model_loaded": model_loaded,
        "load_error": ai_engine._load_error if not model_loaded else None,
    }


@app.post("/reload-model")
async def reload_model():
    """Force reload the AI model without restarting the service."""
    ai_engine.reload_model()
    return {
        "model_loaded": ai_engine.is_model_loaded,
        "load_error": ai_engine._load_error,
    }


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("COUNT_BATTERIES_PORT", "8001"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
