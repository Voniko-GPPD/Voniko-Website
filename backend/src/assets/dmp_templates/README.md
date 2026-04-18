# DMP Excel Template Tags

Use `{{TAG_NAME}}` placeholders directly in any sheet cell.

## Scalar tags

- `{{BATCH_ID}}`
- `{{MODEL}}`
- `{{DATE}}`
- `{{DISCHARGE_PATTERN}}`
- `{{CHANNEL}}`
- `{{VOLT_MAX}}`
- `{{VOLT_MIN}}`
- `{{VOLT_AVG}}`
- `{{IM_MAX}}`
- `{{IM_MIN}}`
- `{{IM_AVG}}`

## Array block: `HISTORY_DATA`

To render telemetry rows, create one template row whose first non-empty cell is `{{#HISTORY_DATA}}` and last non-empty cell is `{{/HISTORY_DATA}}`.

Inside the same row, use telemetry tags:

- `{{TIM}}`
- `{{VOLT}}`
- `{{Im}}`
- `{{BATY}}`

The engine duplicates that row for every item in `HISTORY_DATA`, replaces inner tags from each item, and removes the open/close block tags.

## Engine rules

- No hardcoded cell coordinates are used.
- All sheets and rows are scanned dynamically.
- Scalar tags are replaced from the top-level context object.
- Array block tags are expanded only for row-based blocks (`{{#...}}` and `{{/...}}` on the same row).
