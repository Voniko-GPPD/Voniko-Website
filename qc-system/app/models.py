from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import relationship

from .database import Base


class TimestampMixin:
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class SystemSetting(Base):
    __tablename__ = "system_settings"

    key = Column(String(128), primary_key=True)
    value = Column(String(255), nullable=False)


class LineMapping(Base, TimestampMixin):
    __tablename__ = "line_mapping"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(4), nullable=False, unique=True)
    line_desc = Column(String(64), nullable=False)
    battery_model = Column(String(64), nullable=True)
    status = Column(Boolean, nullable=False, default=True)


class DefectType(Base, TimestampMixin):
    __tablename__ = "defect_types"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(64), nullable=False, unique=True)
    status = Column(Boolean, nullable=False, default=True)


class YearMapping(Base, TimestampMixin):
    __tablename__ = "year_mapping"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(4), nullable=False, unique=True)
    year_value = Column(Integer, nullable=False)
    status = Column(Boolean, nullable=False, default=True)


class MonthMapping(Base, TimestampMixin):
    __tablename__ = "month_mapping"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(4), nullable=False, unique=True)
    month_value = Column(Integer, nullable=False)
    status = Column(Boolean, nullable=False, default=True)


class GradeMapping(Base, TimestampMixin):
    __tablename__ = "grade_mapping"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(4), nullable=False, unique=True)
    grade_desc = Column(String(64), nullable=False)
    status = Column(Boolean, nullable=False, default=True)


class SuffixMapping(Base, TimestampMixin):
    __tablename__ = "suffix_mapping"

    id = Column(Integer, primary_key=True, autoincrement=True)
    suffix_code = Column(String(4), nullable=False, unique=True)
    status_desc = Column(String(64), nullable=False)
    status = Column(Boolean, nullable=False, default=True)


class QualityRecord(Base, TimestampMixin):
    __tablename__ = "quality_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    record_time = Column(DateTime, nullable=False, server_default=func.now())
    detected_date = Column(Date, nullable=False)
    upper_code = Column(String(32), nullable=False)
    lower_code = Column(String(32), nullable=False)
    found_department = Column(String(64), nullable=True)
    ocv = Column(String(32), nullable=True)
    building_no = Column(String(32), nullable=True)
    parsed_line = Column(String(16), nullable=False)
    parsed_line_code = Column(String(16), nullable=False, index=True)
    parsed_line_desc = Column(String(64), nullable=False)
    parsed_battery_model = Column(String(64), nullable=True, index=True)
    parsed_station_no = Column(String(2), nullable=False, index=True)
    parsed_production_time = Column(DateTime, nullable=False)
    parsed_grade = Column(String(64), nullable=False)
    parsed_special_status = Column(String(64), nullable=False)
    photo_url = Column(String(255), nullable=True)
    defect_description = Column(String(255), nullable=True)
    defect_type_id = Column(Integer, ForeignKey("defect_types.id"), nullable=False, index=True)
    operator_name = Column(String(64), nullable=False)

    defect_type = relationship("DefectType")


class ProductionOutput(Base, TimestampMixin):
    __tablename__ = "production_outputs"
    __table_args__ = (
        UniqueConstraint("year", "month", "line_code", name="uq_production_outputs_period_line"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer, nullable=False, index=True)
    month = Column(Integer, nullable=False, index=True)
    line_code = Column(String(16), nullable=False, index=True)
    line_desc = Column(String(64), nullable=True)
    battery_model = Column(String(64), nullable=True, index=True)
    output_qty = Column(Integer, nullable=False, default=0)
    note = Column(String(255), nullable=True)
