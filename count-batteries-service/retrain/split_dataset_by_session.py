#!/usr/bin/env python3
"""Split capture metadata by session/group to reduce leakage.

Input CSV requires columns:
- image_path
- capture_session (default group column, configurable)

Output:
- metadata CSV with added `split` column
- optional train/val/test image list files
"""

from __future__ import annotations

import argparse
import csv
import random
from pathlib import Path


REQUIRED_BASE_COLUMNS = {"image_path"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Split dataset by capture session/group.")
    parser.add_argument("--metadata-csv", required=True, help="Input metadata CSV")
    parser.add_argument("--group-column", default="capture_session", help="Group column name")
    parser.add_argument("--train-ratio", type=float, default=0.7)
    parser.add_argument("--val-ratio", type=float, default=0.15)
    parser.add_argument("--test-ratio", type=float, default=0.15)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-csv", required=True, help="Output CSV with split column")
    parser.add_argument(
        "--output-lists-dir",
        default=None,
        help="Optional directory to write train.txt / val.txt / test.txt",
    )
    return parser.parse_args()


def validate_ratios(train_ratio: float, val_ratio: float, test_ratio: float) -> None:
    total = train_ratio + val_ratio + test_ratio
    if abs(total - 1.0) > 1e-6:
        raise ValueError(f"Ratios must sum to 1.0 (got {total})")


def load_rows(path: Path, group_column: str) -> list[dict]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("CSV is missing header")
        fieldnames = set(reader.fieldnames)
        required = REQUIRED_BASE_COLUMNS | {group_column}
        missing = required.difference(fieldnames)
        if missing:
            raise ValueError(f"CSV missing required columns: {sorted(missing)}")
        rows = list(reader)

    if not rows:
        raise ValueError("CSV contains no rows")
    return rows


def split_groups(groups: list[str], train_ratio: float, val_ratio: float) -> dict[str, str]:
    total = len(groups)
    if total < 3:
        raise ValueError("Need at least 3 groups to create train/val/test by group")

    train_end = max(1, int(round(total * train_ratio)))
    val_count = max(1, int(round(total * val_ratio)))
    val_end = train_end + val_count

    if val_end >= total:
        val_end = total - 1
    if train_end >= val_end:
        train_end = max(1, val_end - 1)

    mapping: dict[str, str] = {}
    for g in groups[:train_end]:
        mapping[g] = "train"
    for g in groups[train_end:val_end]:
        mapping[g] = "val"
    for g in groups[val_end:]:
        mapping[g] = "test"
    return mapping


def write_lists(rows: list[dict], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    buckets = {"train": [], "val": [], "test": []}
    for row in rows:
        buckets[row["split"]].append(row["image_path"])

    for split, items in buckets.items():
        list_path = output_dir / f"{split}.txt"
        content = "\n".join(items)
        if content:
            content += "\n"
        list_path.write_text(content, encoding="utf-8")


def main() -> int:
    args = parse_args()
    validate_ratios(args.train_ratio, args.val_ratio, args.test_ratio)

    metadata_path = Path(args.metadata_csv).resolve()
    output_csv_path = Path(args.output_csv).resolve()

    if not metadata_path.exists():
        raise FileNotFoundError(f"Input metadata CSV not found: {metadata_path}")

    rows = load_rows(metadata_path, args.group_column)

    groups = sorted({row[args.group_column] for row in rows if row[args.group_column]})
    if len(groups) < 3:
        raise ValueError(
            f"Need >=3 non-empty groups in column '{args.group_column}', got {len(groups)}"
        )

    rng = random.Random(args.seed)
    rng.shuffle(groups)

    group_to_split = split_groups(groups, args.train_ratio, args.val_ratio)

    split_counts = {"train": 0, "val": 0, "test": 0}
    for row in rows:
        group = row[args.group_column]
        if group not in group_to_split:
            raise ValueError(
                f"Row has empty or unknown group '{group}' in column '{args.group_column}'"
            )
        split = group_to_split[group]
        row["split"] = split
        split_counts[split] += 1

    output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with output_csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    if args.output_lists_dir:
        write_lists(rows, Path(args.output_lists_dir).resolve())

    print("=== Split summary ===")
    print(f"Input: {metadata_path}")
    print(f"Output: {output_csv_path}")
    print(f"Group column: {args.group_column}")
    print(f"Groups: {len(groups)}")
    print(
        "Rows by split: "
        f"train={split_counts['train']}, "
        f"val={split_counts['val']}, "
        f"test={split_counts['test']}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
