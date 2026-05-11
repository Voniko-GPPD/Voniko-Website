"""
Database configuration for Count Batteries Service.
Uses SQLite via SQLAlchemy – no MySQL dependency.
User identity comes from proxy headers set by the Node.js backend.
"""
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os

DATA_DIR = os.getenv("COUNT_BATTERIES_DATA_DIR", "./data")
os.makedirs(DATA_DIR, exist_ok=True)

DATABASE_URL = f"sqlite:///{DATA_DIR}/count_batteries.db"

engine = create_engine(
    DATABASE_URL,
    echo=False,
    connect_args={"check_same_thread": False},
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class DetectionRecord(Base):
    """Record of each battery detection/counting operation."""
    __tablename__ = "detection_records"

    id = Column(Integer, primary_key=True, index=True)
    # User info forwarded from the Voniko-Website Node.js backend
    user_id = Column(String(100), nullable=True)
    username = Column(String(100), nullable=True)
    user_role = Column(String(20), nullable=True)
    count = Column(Integer, nullable=False, default=0)
    result_image_path = Column(String(255))
    po_number = Column(String(100), nullable=True)
    device_info = Column(String(255))
    notes = Column(Text)
    created_at = Column(DateTime, default=datetime.now)

    def __repr__(self):
        return f"<Detection #{self.id}: {self.count} batteries by {self.username}>"


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    Base.metadata.create_all(bind=engine)
    print("Count Batteries DB tables created (SQLite).")
