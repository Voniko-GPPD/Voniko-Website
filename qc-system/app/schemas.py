from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, Field


class LineMappingBase(BaseModel):
    code: str = Field(..., min_length=1, max_length=4)
    line_desc: str = Field(..., max_length=64)
    battery_model: Optional[str] = Field(default=None, max_length=64)
    status: bool = True


class LineMappingCreate(LineMappingBase):
    pass


class LineMappingUpdate(BaseModel):
    code: Optional[str] = Field(default=None, min_length=1, max_length=4)
    line_desc: Optional[str] = Field(default=None, max_length=64)
    battery_model: Optional[str] = Field(default=None, max_length=64)
    status: Optional[bool] = None


class LineMappingOut(LineMappingBase):
    id: int

    model_config = {"from_attributes": True}


class DefectTypeBase(BaseModel):
    name: str = Field(..., max_length=64)
    status: bool = True


class DefectTypeCreate(DefectTypeBase):
    pass


class DefectTypeUpdate(BaseModel):
    name: Optional[str] = Field(default=None, max_length=64)
    status: Optional[bool] = None


class DefectTypeOut(DefectTypeBase):
    id: int

    model_config = {"from_attributes": True}


class YearMappingBase(BaseModel):
    code: str = Field(..., min_length=1, max_length=4)
    year_value: int
    status: bool = True


class YearMappingCreate(YearMappingBase):
    pass


class YearMappingUpdate(BaseModel):
    code: Optional[str] = Field(default=None, min_length=1, max_length=4)
    year_value: Optional[int] = None
    status: Optional[bool] = None


class YearMappingOut(YearMappingBase):
    id: int

    model_config = {"from_attributes": True}


class MonthMappingBase(BaseModel):
    code: str = Field(..., min_length=1, max_length=4)
    month_value: int = Field(..., ge=1, le=12)
    status: bool = True


class MonthMappingCreate(MonthMappingBase):
    pass


class MonthMappingUpdate(BaseModel):
    code: Optional[str] = Field(default=None, min_length=1, max_length=4)
    month_value: Optional[int] = Field(default=None, ge=1, le=12)
    status: Optional[bool] = None


class MonthMappingOut(MonthMappingBase):
    id: int

    model_config = {"from_attributes": True}


class GradeMappingBase(BaseModel):
    code: str = Field(..., min_length=1, max_length=4)
    grade_desc: str = Field(..., max_length=64)
    status: bool = True


class GradeMappingCreate(GradeMappingBase):
    pass


class GradeMappingUpdate(BaseModel):
    code: Optional[str] = Field(default=None, min_length=1, max_length=4)
    grade_desc: Optional[str] = Field(default=None, max_length=64)
    status: Optional[bool] = None


class GradeMappingOut(GradeMappingBase):
    id: int

    model_config = {"from_attributes": True}


class SuffixMappingBase(BaseModel):
    suffix_code: str = Field(..., max_length=4)
    status_desc: str = Field(..., max_length=64)
    status: bool = True


class SuffixMappingCreate(SuffixMappingBase):
    pass


class SuffixMappingUpdate(BaseModel):
    suffix_code: Optional[str] = Field(default=None, max_length=4)
    status_desc: Optional[str] = Field(default=None, max_length=64)
    status: Optional[bool] = None


class SuffixMappingOut(SuffixMappingBase):
    id: int

    model_config = {"from_attributes": True}


class ParseRequest(BaseModel):
    upper_code: str
    lower_code: str


class ParseResponse(BaseModel):
    production_line: str
    line_desc: str
    battery_model: Optional[str] = None
    production_time: datetime
    grade: str
    special_status: str
    station_no: str            # 2-digit station number e.g. "03"
    year: int
    month: int
    day: int
    hour: int
    minute: int
    line_code: str
    year_code: str
    month_code: str
    grade_code: str
    suffix: Optional[str] = None
    suffix_tokens: list[str] = []


class QualityRecordCreate(BaseModel):
    detected_date: date
    upper_code: str
    lower_code: str
    found_department: Optional[str] = Field(default=None, max_length=64)
    ocv: Optional[str] = Field(default=None, max_length=32)
    building_no: Optional[str] = Field(default=None, max_length=32)
    defect_type_id: int
    defect_description: Optional[str] = Field(default=None, max_length=255)
    operator_name: str = Field(..., max_length=64)


class QualityRecordOut(BaseModel):
    id: int
    record_time: datetime
    detected_date: date
    upper_code: str
    lower_code: str
    found_department: Optional[str] = None
    ocv: Optional[str] = None
    building_no: Optional[str] = None
    parsed_line: str
    parsed_line_code: str
    parsed_line_desc: str
    parsed_battery_model: Optional[str] = None
    parsed_station_no: str
    parsed_production_time: datetime
    parsed_grade: str
    parsed_special_status: str
    photo_url: Optional[str] = None
    defect_description: Optional[str] = None
    defect_type_id: int
    defect_type_name: str
    operator_name: str

    model_config = {"from_attributes": True}


class OCRIngestRequest(BaseModel):
    upper_code: str
    lower_code: str
    detected_date: Optional[date] = None
    found_department: Optional[str] = Field(default=None, max_length=64)
    ocv: Optional[str] = Field(default=None, max_length=32)
    building_no: Optional[str] = Field(default=None, max_length=32)
    defect_type_id: Optional[int] = None
    operator_name: Optional[str] = None
    auto_save: bool = False


class OCRIngestResponse(BaseModel):
    parsed: ParseResponse
    saved_record_id: Optional[int] = None


class MonthlySummaryRow(BaseModel):
    month: int
    defect_type_id: int
    defect_name: str
    count: int


class YearlySummaryRow(BaseModel):
    year: int
    defect_type_id: int
    defect_name: str
    count: int


class ProductionOutputBase(BaseModel):
    year: int = Field(..., ge=2000, le=2100)
    month: int = Field(..., ge=1, le=12)
    line_code: str = Field(..., min_length=1, max_length=16)
    line_desc: Optional[str] = Field(default=None, max_length=64)
    battery_model: Optional[str] = Field(default=None, max_length=64)
    output_qty: int = Field(..., ge=0)
    note: Optional[str] = Field(default=None, max_length=255)


class ProductionOutputCreate(ProductionOutputBase):
    pass


class ProductionOutputUpdate(BaseModel):
    year: Optional[int] = Field(default=None, ge=2000, le=2100)
    month: Optional[int] = Field(default=None, ge=1, le=12)
    line_code: Optional[str] = Field(default=None, min_length=1, max_length=16)
    line_desc: Optional[str] = Field(default=None, max_length=64)
    battery_model: Optional[str] = Field(default=None, max_length=64)
    output_qty: Optional[int] = Field(default=None, ge=0)
    note: Optional[str] = Field(default=None, max_length=255)


class ProductionOutputOut(ProductionOutputBase):
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class MonthlyPpmRow(BaseModel):
    year: int
    month: int
    line_code: str
    line_desc: Optional[str] = None
    battery_model: Optional[str] = None
    defect_count: int
    output_qty: int
    ppm: float
