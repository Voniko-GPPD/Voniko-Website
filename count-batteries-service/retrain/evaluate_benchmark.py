#!/usr/bin/env python3
"""Evaluate tray-count KPI from a benchmark CSV.

Required CSV columns:
- ground_truth_count
- predicted_count

Optional columns:
- tray_id
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


REQUIRED_COLUMNS = {"ground_truth_count", "predicted_count"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate battery count benchmark KPI.")
    parser.add_argument("--csv", required=True, help="Path to benchmark CSV")
    parser.add_argument("--tolerance", type=int, default=2, help="Allowed absolute error (default: 2)")
    parser.add_argument("--max-mae", type=float, default=None, help="Optional MAE gate")
    parser.add_argument(
        "--max-exceed-rate",
        type=float,
        default=None,
        help="Optional gate for exceed rate in percent",
    )
    parser.add_argument("--output-json", default=None, help="Optional path to write KPI JSON")
    return parser.parse_args()


def load_rows(csv_path: Path) -> list[dict]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("CSV is missing header")
        missing = REQUIRED_COLUMNS.difference(set(reader.fieldnames))
        if missing:
            raise ValueError(f"CSV missing required columns: {sorted(missing)}")
        rows = list(reader)

    if not rows:
        raise ValueError("CSV contains no data rows")
    return rows


def evaluate(rows: list[dict], tolerance: int) -> dict:
    abs_errors: list[float] = []
    exceed_count = 0

    for i, row in enumerate(rows, start=1):
        try:
            gt = float(row["ground_truth_count"])
            pred = float(row["predicted_count"])
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid numeric value at row {i}") from exc

        abs_err = abs(pred - gt)
        abs_errors.append(abs_err)
        if abs_err > tolerance:
            exceed_count += 1

    sample_count = len(abs_errors)
    mae = sum(abs_errors) / sample_count
    exceed_rate = exceed_count * 100.0 / sample_count

    return {
        "sample_count": sample_count,
        "tolerance": tolerance,
        "mae": mae,
        "exceed_count": exceed_count,
        "exceed_rate_percent": exceed_rate,
    }


def main() -> int:
    args = parse_args()
    csv_path = Path(args.csv).resolve()

    if not csv_path.exists():
        raise FileNotFoundError(f"Benchmark CSV not found: {csv_path}")

    rows = load_rows(csv_path)
    result = evaluate(rows, args.tolerance)

    print("=== Benchmark KPI ===")
    print(f"CSV: {csv_path}")
    print(f"Samples: {result['sample_count']}")
    print(f"MAE: {result['mae']:.4f}")
    print(
        f"Exceed rate (>|±{result['tolerance']}|): "
        f"{result['exceed_rate_percent']:.2f}% ({result['exceed_count']}/{result['sample_count']})"
    )

    if args.output_json:
        output_path = Path(args.output_json).resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
        print(f"Saved KPI JSON: {output_path}")

    failed = False
    if args.max_mae is not None and result["mae"] > args.max_mae:
        print(f"FAIL: MAE {result['mae']:.4f} > {args.max_mae}")
        failed = True
    if args.max_exceed_rate is not None and result["exceed_rate_percent"] > args.max_exceed_rate:
        print(
            "FAIL: exceed rate "
            f"{result['exceed_rate_percent']:.2f}% > {args.max_exceed_rate}%"
        )
        failed = True

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
