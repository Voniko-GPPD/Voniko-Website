import re
from datetime import datetime

from sqlalchemy.orm import Session

from .. import models

# Upper format:
# [line_code(1-4)][year(1)][month(1)][grade(1-4)][station(2 digits)] + optional suffixes like -B-J
UPPER_CODE_PATTERN = re.compile(r"^([A-Z0-9+]{6,14})((?:-[A-Z])+)?$")
LOWER_CODE_PATTERN = re.compile(r"^\d{6}$")
SUFFIX_TOKEN_PATTERN = re.compile(r"-[A-Z]")


class ParseCodeError(ValueError):
    pass


def _normalize_upper_code(value: str) -> str:
    # Tolerate manual input with spaces: "V1GZ H03-B-J"
    return value.strip().upper().replace(" ", "")


def _normalize_lower_code(value: str) -> str:
    return value.strip()


def _lookup_mapping(db: Session, model, code_field: str, code_value: str, position_name: str):
    record = (
        db.query(model)
        .filter(getattr(model, code_field) == code_value, model.status.is_(True))
        .first()
    )
    if not record:
        raise ParseCodeError(f"{position_name}代码 '{code_value}' 未在后台维护，请联系管理员")
    return record


def _parse_upper_segments(db: Session, base_code: str):
    # Find enabled line mappings by prefix and prefer the longest prefix.
    line_candidates = (
        db.query(models.LineMapping)
        .filter(models.LineMapping.status.is_(True))
        .all()
    )
    matched_lines = [
        line for line in line_candidates
        if base_code.startswith(line.code) and 5 <= len(base_code) - len(line.code) <= 8
    ]
    if not matched_lines:
        raise ParseCodeError("产线代码未在后台维护，请先在生产线映射中新增并启用")

    matched_lines.sort(key=lambda x: len(x.code), reverse=True)
    line_mapping = matched_lines[0]
    rest = base_code[len(line_mapping.code):]

    year_code = rest[0]
    month_code = rest[1]
    grade_code = rest[2:-2]
    station_no = rest[-2:]

    if not (1 <= len(grade_code) <= 4):
        raise ParseCodeError("等级代码长度无效，应为 1-4 位")
    if not station_no.isdigit() or int(station_no) > 50:
        raise ParseCodeError(f"工位号 '{station_no}' 无效，应为 00-50 之间的两位数字")

    return line_mapping, year_code, month_code, grade_code, station_no


def parse_battery_codes(db: Session, upper_code: str, lower_code: str) -> dict:
    normalized_upper = _normalize_upper_code(upper_code)
    normalized_lower = _normalize_lower_code(lower_code)

    upper_match = UPPER_CODE_PATTERN.match(normalized_upper)
    if not upper_match:
        raise ParseCodeError(
            "上排喷码格式错误，应为：产线码(1-4位) + 年份(1位) + 月份(1位) + 等级(1-4位) + 工位(2位数字) + 可选后缀"
            "（示例：V1GZH03 或 V1GZH03-B-J）"
        )
    if not LOWER_CODE_PATTERN.match(normalized_lower):
        raise ParseCodeError("下排喷码格式错误，应为6位数字（DDHHMM）")

    base_code = upper_match.group(1)
    suffix_str = upper_match.group(2) or ""

    line_mapping, year_code, month_code, grade_code, station_no = _parse_upper_segments(db, base_code)
    year_mapping = _lookup_mapping(db, models.YearMapping, "code", year_code, "年份")
    month_mapping = _lookup_mapping(db, models.MonthMapping, "code", month_code, "月份")
    grade_mapping = _lookup_mapping(db, models.GradeMapping, "code", grade_code, "等级")

    suffix_tokens = SUFFIX_TOKEN_PATTERN.findall(suffix_str)
    if suffix_tokens:
        desc_parts = []
        for token in suffix_tokens:
            sm = _lookup_mapping(db, models.SuffixMapping, "suffix_code", token, "后缀")
            desc_parts.append(sm.status_desc)
        special_status = " / ".join(desc_parts)
    else:
        special_status = "正常"

    day = int(normalized_lower[0:2])
    hour = int(normalized_lower[2:4])
    minute = int(normalized_lower[4:6])

    try:
        production_time = datetime(
            year_mapping.year_value,
            month_mapping.month_value,
            day,
            hour,
            minute,
        )
    except ValueError as exc:
        raise ParseCodeError(f"下排喷码解析失败：{exc}") from exc

    return {
        "production_line": f"{line_mapping.line_desc}({line_mapping.code})",
        "line_desc": line_mapping.line_desc,
        "battery_model": line_mapping.battery_model,
        "production_time": production_time,
        "grade": grade_mapping.grade_desc,
        "special_status": special_status,
        "station_no": station_no,
        "year": year_mapping.year_value,
        "month": month_mapping.month_value,
        "day": day,
        "hour": hour,
        "minute": minute,
        "line_code": line_mapping.code,
        "year_code": year_code,
        "month_code": month_code,
        "grade_code": grade_code,
        "suffix": suffix_str or None,
        "suffix_tokens": suffix_tokens,
    }
