# Count Batteries Service

Python FastAPI micro-service for AI-based battery counting.  
Integrated with the Voniko-Website platform.

## Overview

- Runs on **port 8001** (separate from the battery-test service on 8765)
- Auth is delegated to the Node.js backend (`x-user-id`, `x-username`, `x-user-role` headers)
- Database: **SQLite** at `./data/count_batteries.db` (no MySQL/XAMPP needed)
- AI model: YOLOv8 exported to ONNX – place `best.onnx` in `count-batteries-service/models/`

## Setup

```bash
cd count-batteries-service
pip install -r requirements.txt
```

### ONNX model

Copy `best.onnx` (and optionally `best_fp16.onnx`) to `count-batteries-service/models/`:

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

## Accuracy Tuning Notes (Battery Tray Counting)

### 1) Runtime tuning (quick wins)

- The API still accepts `confidence` from UI (slider), and this user-selected threshold is respected in both standard and SAHI paths.
- Adaptive confidence remains as fallback when the caller does not provide a threshold.
- Post-processing duplicate suppression is tuned to reduce over-count from overlapping detections.

### 2) Image capture standardization

To stabilize counting before retraining:

- Keep camera angle fixed (top-down) and fixed tray distance.
- Use uniform lighting across the tray; avoid strong shadows on tray edges.
- Avoid motion blur; keep focus locked.
- Keep tray fully visible with minimal background clutter.

### 3) Retraining workflow (YOLO → ONNX)

1. Collect real production images from multiple conditions:
   - bright / low light
   - different tray batches
   - edge zones and reflective-metal backgrounds
2. Label consistently for every battery center/object, especially edge and dark zones.
3. Split train/val/test by capture sessions/shifts (to reduce overfit).
4. Train YOLO model with updated dataset.
5. Export ONNX and replace:
   - `count-batteries-service/models/best.onnx`
6. Reload model without full restart:
   - call `POST /reload-model` (already supported)

### 4) Acceptance KPI

Validate per tray against manual ground truth:

- MAE (mean absolute error) in battery count
- error rate of trays exceeding an allowed tolerance (e.g. `>|±N|`)

Only promote the model/config when KPI meets production threshold.

## Practical Retrain Execution (Production)

This section turns the retrain checklist into a runnable flow for this repository.

### A) Freeze baseline before retrain

1. Keep current production model as baseline:
   - `count-batteries-service/models/best.onnx`
2. Create a fixed benchmark CSV (manual counts already verified):
   - Start from: `count-batteries-service/retrain/templates/benchmark_baseline_template.csv`
3. Evaluate and lock KPI gates:

```bash
cd count-batteries-service
python retrain/evaluate_benchmark.py \
  --csv /absolute/path/to/benchmark_baseline.csv \
  --tolerance 2 \
  --max-mae 1.5 \
  --max-exceed-rate 10 \
  --output-json /absolute/path/to/kpi_baseline.json
```

### B) Standardize real capture metadata

Use this template for real production captures:
- `count-batteries-service/retrain/templates/capture_metadata_template.csv`

Required minimum fields for split tooling:
- `image_path`
- `capture_session`

Recommended operational fields:
- `capture_date`, `shift`, `camera_id`, `lighting_condition`, `tray_type`, `notes`

### C) Split by session to reduce overfit

Prepare metadata split (train/val/test by session, not random image split):

```bash
cd count-batteries-service
python retrain/split_dataset_by_session.py \
  --metadata-csv /absolute/path/to/capture_metadata.csv \
  --group-column capture_session \
  --train-ratio 0.7 \
  --val-ratio 0.15 \
  --test-ratio 0.15 \
  --output-csv /absolute/path/to/capture_metadata_split.csv \
  --output-lists-dir /absolute/path/to/splits
```

### D) Dataset structure (YOLO)

Use this standard structure:

```text
<dataset_root>/
  images/
    train/
    val/
    test/
  labels/
    train/
    val/
    test/
  metadata/
    capture_metadata.csv
    capture_metadata_split.csv
```

Dataset YAML template:
- `count-batteries-service/retrain/templates/dataset_yaml_template.yaml`

### E) Prioritized train parameters (real trays)

Run multiple candidates and compare by counting KPI (not only detection mAP):

```bash
yolo detect train \
  model=yolov8n.pt \
  data=/absolute/path/to/dataset.yaml \
  imgsz=1280 \
  epochs=120 \
  batch=8 \
  device=0 \
  workers=4 \
  cos_lr=True \
  close_mosaic=10 \
  hsv_h=0.015 hsv_s=0.7 hsv_v=0.4 \
  degrees=0.0 translate=0.05 scale=0.20 shear=0.0 \
  fliplr=0.5 mosaic=1.0 mixup=0.1 \
  project=/absolute/path/to/runs \
  name=tray_retrain_candidate_01
```

Recommended candidate sweep order:
1. `imgsz`: 1280 (primary), then 1024 for speed check
2. `batch`: highest stable value on target GPU
3. `epochs`: 80 / 120 / 160
4. augmentation intensity: light vs medium

### F) Export ONNX and deploy to service

1. Export best candidate:

```bash
yolo export \
  model=/absolute/path/to/best.pt \
  format=onnx \
  imgsz=1280 \
  dynamic=False \
  simplify=True
```

2. Replace service model:
   - `count-batteries-service/models/best.onnx`
3. Hot reload without full restart:

```bash
curl -X POST http://localhost:8001/reload-model
```

### G) Acceptance checklist by milestone

- [ ] Baseline benchmark fixed and versioned
- [ ] Capture metadata complete for all new real images
- [ ] Split by session/day/shift, test set frozen
- [ ] At least 2 candidate models trained
- [ ] Candidate compared on benchmark KPI (MAE + exceed-rate)
- [ ] ONNX deployed to `count-batteries-service/models/best.onnx`
- [ ] `/reload-model` successful and `/health` confirms model loaded
- [ ] Post-deploy benchmark passes KPI gates
