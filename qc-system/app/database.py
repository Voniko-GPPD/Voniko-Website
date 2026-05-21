import os
from pathlib import Path

from sqlalchemy import create_engine, event
from sqlalchemy.orm import declarative_base, sessionmaker

_BASE = Path(__file__).resolve().parent.parent.parent
_DATA_DIR = _BASE / "backend" / "data"
_DATA_DIR.mkdir(parents=True, exist_ok=True)

DATABASE_URL = (
    os.getenv("QC_DATABASE_URL")
    or os.getenv("DATABASE_URL")
    or f"sqlite:///{_DATA_DIR / 'bqms.db'}"
)

if DATABASE_URL.startswith("sqlite"):
    engine = create_engine(
        DATABASE_URL,
        connect_args={"check_same_thread": False},
    )

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()
else:
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
