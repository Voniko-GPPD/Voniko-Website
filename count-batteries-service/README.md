# Count Batteries Service

Python FastAPI micro-service for AI-based battery counting.  
Integrated with the Voniko-Website platform.

## Overview

- Runs on **port 8001** (separate from the battery-test service on 8765)
- Auth is delegated to the Node.js backend (`x-user-id`, `x-username`, `x-user-role` headers)
- Database: **SQLite** at `./data/count_batteries.db` (no MySQL/XAMPP needed)
- AI model: YOLOv8 exported to ONNX – place `best.onnx` in the `../models/` directory

## Setup

```bash
cd count-batteries-service
pip install -r requirements.txt
```

### ONNX model

Copy `best.onnx` (and optionally `best_fp16.onnx`) to `../models/`:

```
models/
└── best.onnx
```

Or set the env var `COUNT_BATTERIES_MODELS_DIR` to your model directory.

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `COUNT_BATTERIES_PORT` | `8001` | Listening port |
| `COUNT_BATTERIES_DATA_DIR` | `./data` | SQLite DB directory |
| `COUNT_BATTERIES_STATIC_DIR` | `./static` | Result images directory |
| `COUNT_BATTERIES_MODELS_DIR` | `../models` | ONNX model directory |
| `COUNT_BATTERIES_CORS_ORIGINS` | `http://localhost:3001,...` | Allowed CORS origins |

## Run

```bash
uvicorn main:app --host 0.0.0.0 --port 8001 --reload
```

Or via the root `start.bat` which starts all services together.
