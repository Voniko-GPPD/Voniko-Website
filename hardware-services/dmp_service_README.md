# DMP Battery Data Bridge — Client Service

## Overview
`dmp_service.py` runs on the Windows machine where the Ee-Share DMP software is installed.
It reads the local `.mdb` database files (via pyodbc + shadow copy) and registers itself to
the central Voniko Node.js server, making the data accessible via the web interface.

## Requirements
- Windows (required for MS Access ODBC driver)
- Python 3.9+
- Microsoft Access Database Engine 2016 Redistributable (32-bit or 64-bit to match Python)
  Download: https://www.microsoft.com/en-us/download/details.aspx?id=54920

## Setup
```bash
cd hardware-services
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

## Environment Variables
| Variable | Description | Default |
|----------|-------------|---------|
| `DMP_STATION_NAME` | Display name for this station (e.g. "DMP Station - Line A") | required |
| `VONIKO_SERVER_URL` | URL of the central Voniko server (e.g. `http://10.4.1.11:3001`) | required |
| `DMP_STATION_PORT` | Port this service listens on | `8766` |
| `DMP_DATA_DIR` | Path to the DMP data directory containing `DMPDATA.mdb` | `C:\DMP\Data` |
| `DMP_TEMPLATES_DIR` | Path to Excel report templates directory | `./dmp_templates` |

## Run
```bash
set DMP_STATION_NAME=DMP Station - Line A
set VONIKO_SERVER_URL=http://10.4.1.11:3001
set DMP_DATA_DIR=C:\DMP\Data
uvicorn dmp_service:app --host 0.0.0.0 --port 8766
```

## How it works
1. The service starts and begins sending heartbeats to `{VONIKO_SERVER_URL}/api/dmp/register` every 30 seconds.
2. The Voniko web interface detects the station as "online" and shows it in the station selector.
3. Users select the DMP station in the web UI to browse battery test data.
4. All `.mdb` file reads use a shadow copy mechanism to avoid conflicts with the running DMP software.
