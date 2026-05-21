PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS line_mapping (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL UNIQUE,
    line_desc TEXT NOT NULL,
    battery_model TEXT,
    status INTEGER NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS defect_types (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    status INTEGER NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS year_mapping (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL UNIQUE,
    year_value INTEGER NOT NULL,
    status INTEGER NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS month_mapping (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL UNIQUE,
    month_value INTEGER NOT NULL,
    status INTEGER NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS grade_mapping (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL UNIQUE,
    grade_desc TEXT NOT NULL,
    status INTEGER NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS suffix_mapping (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    suffix_code TEXT NOT NULL UNIQUE,
    status_desc TEXT NOT NULL,
    status INTEGER NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS quality_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    record_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    detected_date DATE NOT NULL,
    upper_code TEXT NOT NULL,
    lower_code TEXT NOT NULL,
    parsed_line TEXT NOT NULL,
    parsed_line_code TEXT NOT NULL,
    parsed_line_desc TEXT NOT NULL,
    parsed_battery_model TEXT,
    parsed_station_no TEXT NOT NULL,
    parsed_production_time DATETIME NOT NULL,
    parsed_grade TEXT NOT NULL,
    parsed_special_status TEXT NOT NULL,
    photo_url TEXT,
    defect_type_id INTEGER NOT NULL,
    operator_name TEXT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (defect_type_id) REFERENCES defect_types(id)
);

CREATE TABLE IF NOT EXISTS production_outputs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    line_code TEXT NOT NULL,
    line_desc TEXT,
    battery_model TEXT,
    output_qty INTEGER NOT NULL DEFAULT 0,
    note TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(year, month, line_code)
);

CREATE INDEX IF NOT EXISTS idx_quality_records_production_time ON quality_records(parsed_production_time);
CREATE INDEX IF NOT EXISTS idx_quality_records_defect_type ON quality_records(defect_type_id);
CREATE INDEX IF NOT EXISTS idx_quality_records_detected_date ON quality_records(detected_date);
CREATE INDEX IF NOT EXISTS idx_quality_records_line_code ON quality_records(parsed_line_code);
CREATE INDEX IF NOT EXISTS idx_quality_records_model ON quality_records(parsed_battery_model);
CREATE INDEX IF NOT EXISTS idx_quality_records_station ON quality_records(parsed_station_no);

CREATE INDEX IF NOT EXISTS idx_production_outputs_period ON production_outputs(year, month);
CREATE INDEX IF NOT EXISTS idx_production_outputs_line ON production_outputs(line_code);
