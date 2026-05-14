"""Unit tests for DM2000 Performance Report condition labelling and matching.

Covers two related fixes:

1. ``_build_dm2000_condition_label`` must format compound resistors
   (``fzdz='620+10k'``) the same way the IEC 60086-2 templates do
   (``620ohm+10Kohm``).

2. ``_perf_fdfs_matches_template`` must tolerate the comma-without-space
   separators DM2000 stores (``4m/h,8h/d`` / ``1s/60m,24h/d``) when matching
   them against IEC template entries that use spaces or dots
   (``4m/h 8h/d`` / ``1s/60m.24h/d``).

Real raw values come from
``Voniko-GPPD/Database/dmdata_ls mdb/ls_jb_cs.xlsx``: ``fzdz`` is a bare
number (``'10'``, ``'3.9'``, …) or the compound string ``'620+10k'``;
``fdfs`` is comma-without-space (``'4m/h,8h/d'``, ``'1s/60m,24h/d'``); ``zzdy``
is the endpoint voltage (``'0.900'``, ``'7.500'``, …).
"""

from __future__ import annotations

import pytest

import dmp_service as m


# --------------------------------------------------------------------------- #
# _normalize_dm2000_load_resistance / _build_dm2000_condition_label
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("", ""),
        ("10", "10ohm"),
        ("3.9", "3.9ohm"),
        ("620", "620ohm"),
        ("180", "180ohm"),
        # Compound resistor (real DM2000 storage form for the 9V everymonth
        # condition): "620+10k" must become "620ohm+10Kohm" (capital K to
        # match the IEC template style and the on-screen header text).
        ("620+10k", "620ohm+10Kohm"),
        ("620+10K", "620ohm+10Kohm"),
        ("1+0.5K", "1ohm+0.5Kohm"),
        # Already-unitised values pass through verbatim.
        ("1000mA", "1000mA"),
        ("(1500mW2s,650mW28s)", "(1500mW2s,650mW28s)"),
        ("620ohm+10Kohm", "620ohm+10Kohm"),
    ],
)
def test_normalize_dm2000_load_resistance(raw: str, expected: str) -> None:
    assert m._normalize_dm2000_load_resistance(raw) == expected


def test_build_label_compound_9v_everymonth() -> None:
    """The 9V everymonth condition is the bug from the user's screenshot."""
    label = m._build_dm2000_condition_label(
        "1s/60m,24h/d", "620+10k", "7.500", "fallback",
    )
    assert label == "620ohm+10Kohm 1s/60m,24h/d-7.500V"


def test_build_label_simple_resistance() -> None:
    assert (
        m._build_dm2000_condition_label("24h/d", "10", "0.900", "x")
        == "10ohm 24h/d-0.900V"
    )


def test_build_label_already_unitised() -> None:
    assert (
        m._build_dm2000_condition_label("24h/d", "1000mA", "0.900", "x")
        == "1000mA 24h/d-0.900V"
    )


def test_build_label_falls_back_when_all_fields_empty() -> None:
    assert m._build_dm2000_condition_label("", "", "", "ARCHNAME") == "ARCHNAME"


# --------------------------------------------------------------------------- #
# _perf_fdfs_matches_template / _get_condition_freq_group
# --------------------------------------------------------------------------- #


# Each entry: (battery_family, fdfs_raw, fzdz_raw, ep_str, expected_group).
# Covers EVERY condition listed in the bug report for LR6 / LR03 / LR61 / 9V
# using the actual raw values DM2000 stores in ls_jb_cs.
_CASES = [
    # 9V
    ("9V", "24h/d",        "35mA",     "5.400", "everyday"),
    ("9V", "4h/d",         "180",      "6.800", "everyweek"),
    ("9V", "1h/d",         "270",      "5.400", "everyweek"),
    ("9V", "2h/d",         "620",      "5.400", "everymonth"),
    ("9V", "1s/60m,24h/d", "620+10k",  "7.500", "everymonth"),
    # LR6
    ("LR6", "24h/d",       "10",       "0.900", "everyday"),
    ("LR6", "24h/d",       "1000mA",   "0.900", "everyday"),
    ("LR6", "1h/d",        "3.9",      "0.800", "everyweek"),
    ("LR6", "4m/h,8h/d",   "3.9",      "0.900", "everyweek"),
    ("LR6", "24h/d",       "3.9",      "0.800", "everymonth"),
    # LR03
    ("LR03", "24h/d",       "20",      "0.900", "everyday"),
    ("LR03", "1h/d",        "5.1",     "0.800", "everyweek"),
    ("LR03", "4m/h,8h/d",   "5.1",     "0.900", "everyweek"),
    ("LR03", "15s/m,8h/d",  "24",      "1.000", "everymonth"),
    ("LR03", "24h/d",       "3.9",     "0.800", "everymonth"),
    # LR61
    ("LR61", "24h/d",       "35mA",    "0.900", "everyday"),
    ("LR61", "5m/d",        "5.1",     "0.900", "everyweek"),
    ("LR61", "1h/d",        "75",      "0.900", "everymonth"),
    ("LR61", "1h/d",        "75",      "1.100", "everymonth"),
]


@pytest.mark.parametrize("family,fdfs,fzdz,ep,expected", _CASES)
def test_built_label_classifies_into_correct_group(
    family: str, fdfs: str, fzdz: str, ep: str, expected: str
) -> None:
    label = m._build_dm2000_condition_label(fdfs, fzdz, ep, "fallback")
    assert m._get_condition_freq_group(label, family) == expected, (
        f"{label!r} should classify as {expected!r} for family {family!r}"
    )


def test_template_match_tolerates_decimal_voltage_precision() -> None:
    # "3.9ohm 24h/d-0.800V" (DM2000 zzdy='0.800') vs template "3.9ohm 24h/d-0.8V"
    assert m._perf_fdfs_matches_template(
        "3.9ohm 24h/d-0.800V", "3.9ohm 24h/d-0.8V"
    )


def test_template_match_tolerates_comma_vs_space_in_schedule() -> None:
    # DM2000 stores "4m/h,8h/d"; template has "4m/h 8h/d".
    assert m._perf_fdfs_matches_template(
        "3.9ohm 4m/h,8h/d-0.900V", "3.9ohm 4m/h 8h/d-0.9V"
    )


def test_template_match_tolerates_comma_vs_dot_in_schedule() -> None:
    # DM2000 stores "1s/60m,24h/d"; template has "1s/60m.24h/d".
    assert m._perf_fdfs_matches_template(
        "620ohm+10Kohm 1s/60m,24h/d-7.500V",
        "620ohm+10Kohm 1s/60m.24h/d-7.5V",
    )


def test_template_match_does_not_collide_on_leading_token() -> None:
    """Regression guard: matcher must NOT treat conditions sharing only the
    leading current/resistance token as equivalent.  This is the original
    reason ``_perf_fdfs_matches_template`` is stricter than
    ``_perf_fdfs_matches_header`` and must not regress."""
    assert not m._perf_fdfs_matches_template(
        "1000mA 10s/m 1h/d-0.9V", "1000mA 24h/d-0.9V"
    )
    assert not m._perf_fdfs_matches_template(
        "100mA 1h/d-0.9V", "1000mA 24h/d-0.9V"
    )
    # Different battery families with different leading resistances must also
    # remain distinguishable.
    assert not m._perf_fdfs_matches_template(
        "20ohm 24h/d-0.9V", "10ohm 24h/d-0.9V"
    )
    assert not m._perf_fdfs_matches_template(
        "5.1ohm 1h/d-0.8V", "3.9ohm 1h/d-0.8V"
    )


# --------------------------------------------------------------------------- #
# Operator remark suffixes (Q / 15) and LR6 daily/15-day routing
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "raw,expected_clean,expected_q,expected_15d",
    [
        # No suffixes
        ("LR6 UD501 UD502", "LR6 UD501 UD502", False, False),
        # Quarterly suffix (uppercase + lowercase)
        ("LR6 UD501 UD502 Q", "LR6 UD501 UD502", True, False),
        ("LR6 UD501 UD502 q", "LR6 UD501 UD502", True, False),
        # 15-day suffix
        ("LR6 UD501 UD502 15", "LR6 UD501 UD502", False, True),
        # Both suffixes, in either order, with extra spaces
        ("LR6 UD501  UD502  Q  15", "LR6 UD501 UD502", True, True),
        ("  LR6 UD501 15 Q  ", "LR6 UD501", True, True),
        # Identifiers that legitimately contain ``15`` or ``Q`` as part of a
        # longer token must not be stripped.
        ("LR6 UD515 UDP502", "LR6 UD515 UDP502", False, False),
        ("LR6 UDQ501 HP503", "LR6 UDQ501 HP503", False, False),
        # Empty / None inputs
        ("", "", False, False),
        (None, "", False, False),
        ("   ", "", False, False),
    ],
)
def test_strip_remark_suffixes(
    raw, expected_clean: str, expected_q: bool, expected_15d: bool
) -> None:
    clean, is_q, is_15d = m._strip_remark_suffixes(raw)
    assert clean == expected_clean
    assert is_q == expected_q
    assert is_15d == expected_15d


def test_lr6_route_fdfs_labels_routes_to_daily_by_default() -> None:
    """Default (is_15d=False) routes the LR6 1500mW2s/650mW28s condition to
    the daily column ONLY — both the bare condition and the legacy
    ``-1.05V``/``-1.0V`` voltage-suffixed forms collapse to the same key.
    Quarterly measurements (``Q`` without ``15``) follow this path so they
    write only into the normal daily column."""
    daily = m._LR6_1500MW_DAILY_LABEL
    for raw in (
        "(1500mW2s,650mW28s)10T/h,24h/d",
        "(1500mW2s,650mW28s)10T/h,24h/d-1.05V",
        "(1500mW2s,650mW28s)10T/h,24h/d-1.0V",
    ):
        assert m._lr6_route_fdfs_labels(raw, "LR6", False) == [daily]


def test_lr6_route_fdfs_labels_writes_only_15d_column_when_15d() -> None:
    """A 15-day measurement (``is_15d=True``) on the LR6 1500mW2s/650mW28s
    condition writes the result into the dedicated 15-day column ONLY;
    it does NOT also write the daily column.  This keeps the two columns
    visually distinct (daily on the left, 15-day on the right) instead of
    having the 15-day value overwrite/duplicate into the daily slot."""
    fifteen = m._LR6_1500MW_15D_LABEL
    for raw in (
        "(1500mW2s,650mW28s)10T/h,24h/d",
        "(1500mW2s,650mW28s)10T/h,24h/d-1.05V",
    ):
        assert m._lr6_route_fdfs_labels(raw, "LR6", True) == [fifteen]


def test_lr6_route_fdfs_labels_only_applies_to_lr6() -> None:
    """The 15-day cadence column is LR6-only — non-LR6 models always pass
    the label through unchanged regardless of is_15d."""
    raw = "(1500mW2s,650mW28s)10T/h,24h/d"
    for fam in ("LR03", "LR61", "9V"):
        assert m._lr6_route_fdfs_labels(raw, fam, True) == [raw]
        assert m._lr6_route_fdfs_labels(raw, fam, False) == [raw]


def test_lr6_route_fdfs_labels_passes_unrelated_conditions_through() -> None:
    """Conditions that don't match the 1500mW2s/650mW28s base condition are
    returned unchanged for LR6 too, regardless of the is_15d flag (the
    15-day column is exclusive to the 1500mW2s/650mW28s condition)."""
    for raw in ("10ohm 24h/d-0.9V", "1000mA 24h/d-0.9V", "3.9ohm 1h/d-0.8V"):
        assert m._lr6_route_fdfs_labels(raw, "LR6", True) == [raw]
        assert m._lr6_route_fdfs_labels(raw, "LR6", False) == [raw]


def test_lr6_template_has_daily_then_15d_slots() -> None:
    """Template ordering: daily column comes immediately before 15-day so
    that the on-screen layout reads ``Daily | 15-day`` left to right."""
    tmpl = m._TEMPLATE_CONDITION_ORDER["LR6"]
    daily_idx = tmpl.index(m._LR6_1500MW_DAILY_LABEL)
    fifteen_idx = tmpl.index(m._LR6_1500MW_15D_LABEL)
    assert daily_idx + 1 == fifteen_idx


def test_lr6_freq_groups_both_in_everyday() -> None:
    """Both daily and 15-day slots are grouped under ``everyday`` so the
    report shows the 15-day column as an extra column to the right of the
    daily column under the same Everyday group header (no separate freq
    group / filter chip)."""
    assert m._get_condition_freq_group(m._LR6_1500MW_DAILY_LABEL, "LR6") == "everyday"
    assert m._get_condition_freq_group(m._LR6_1500MW_15D_LABEL, "LR6") == "everyday"


def test_perf_fdfs_matches_header_does_not_cross_match_daily_and_15d() -> None:
    """Regression test: ``_perf_fdfs_matches_header`` must NOT match the daily
    LR6 1500mW2s/650mW28s fdfs label against the 15D column header (nor the
    15D fdfs label against the daily header).

    Without the ``15D`` guard the whole-word fallback would match because the
    daily label is a whole-word prefix inside the 15D label.  This caused 15D
    data to be written to the daily column and the 15D column to remain empty
    (Requests #237 / #238 regression)."""
    daily = m._LR6_1500MW_DAILY_LABEL
    fifteen = m._LR6_1500MW_15D_LABEL

    # Same label must match itself
    assert m._perf_fdfs_matches_header(daily, daily)
    assert m._perf_fdfs_matches_header(fifteen, fifteen)

    # Cross-match: daily fdfs against 15D header — must NOT match
    assert not m._perf_fdfs_matches_header(daily, fifteen)
    # Cross-match: 15D fdfs against daily header — must NOT match
    assert not m._perf_fdfs_matches_header(fifteen, daily)

    # Voltage-suffixed forms of the daily fdfs still match the daily header
    assert m._perf_fdfs_matches_header(
        "(1500mW2s,650mW28s)10T/h,24h/d-1.05V", daily
    )
    # Voltage-suffixed daily fdfs must NOT match the 15D header
    assert not m._perf_fdfs_matches_header(
        "(1500mW2s,650mW28s)10T/h,24h/d-1.05V", fifteen
    )


def test_perf_fdfs_matches_header_15d_with_embedded_voltage() -> None:
    """Regression: _perf_fdfs_matches_header must match the canonical 15D fdfs
    label (no voltage suffix) against a template column header that embeds the
    voltage suffix *before* the 15D marker.

    The Excel template uses headers like
    "(1500mW2s,650mW28s) 10T/h,24h/d-1.05V 15D" while the canonical routed
    label is "(1500mW2s,650mW28s)10T/h,24h/d 15D" (no voltage, no space after
    ``)``.  Before the fix the end-anchored voltage-strip regex could not strip
    "-1.05V" because " 15D" followed it, so the match returned False and the
    15D column in the generated Excel was left empty (Requests #237/#238/#239).
    """
    fifteen = m._LR6_1500MW_15D_LABEL  # "(1500mW2s,650mW28s)10T/h,24h/d 15D"

    # Canonical 15D label vs template header with embedded voltage
    assert m._perf_fdfs_matches_header(
        fifteen, "(1500mW2s,650mW28s) 10T/h,24h/d-1.05V 15D"
    )
    assert m._perf_fdfs_matches_header(
        fifteen, "(1500mW2s,650mW28s) 10T/h,24h/d-1.0V 15D"
    )

    # Cross-match checks must still be rejected even after the fix
    daily = m._LR6_1500MW_DAILY_LABEL
    assert not m._perf_fdfs_matches_header(
        "(1500mW2s,650mW28s) 10T/h,24h/d-1.05V 15D", daily
    )
    assert not m._perf_fdfs_matches_header(
        daily, "(1500mW2s,650mW28s) 10T/h,24h/d-1.05V 15D"
    )


# --------------------------------------------------------------------------- #
# _merge_bz_suffix_flags — bz column is the canonical source of Q / 15 flags
# --------------------------------------------------------------------------- #


def test_merge_bz_suffix_flags_promotes_15d_from_bz() -> None:
    """A perf-entry whose raw_remark lacks the ``15`` suffix must still be
    routed to the 15D column when the matched para_pub.bz value carries it.

    This is the scenario from Request #241: the operator edits the bz column
    on the DM management page (which writes back to para_pub.bz via
    /update-batch-meta) and expects the perf report to honour the suffix
    without re-creating every dmp_perf_entries row.  The bz column is the
    canonical source of truth for Q / 15 routing.
    """
    # entry has no flag, bz carries the 15 suffix → is_15d must become True
    assert m._merge_bz_suffix_flags(False, False, "LR6 UDP501 15") == (False, True)
    assert m._merge_bz_suffix_flags(False, False, "LR6 UD501 UD502 15") == (False, True)
    # entry has no flag, bz carries the Q suffix → is_quarter must become True
    assert m._merge_bz_suffix_flags(False, False, "LR6 HP501 HP502 Q") == (True, False)
    # entry already True, bz lacks the suffix → entry value is preserved
    assert m._merge_bz_suffix_flags(True, True, "LR6 UDP501") == (True, True)
    # Empty / None bz must be a no-op (no flags fabricated)
    assert m._merge_bz_suffix_flags(False, False, "") == (False, False)
    assert m._merge_bz_suffix_flags(False, False, None) == (False, False)
    assert m._merge_bz_suffix_flags(True, False, None) == (True, False)
    # Composite: both flags promoted from bz
    assert m._merge_bz_suffix_flags(False, False, "LR6 UDP501 Q 15") == (True, True)


def test_merge_bz_suffix_flags_does_not_strip_substring_15() -> None:
    """The ``15`` and ``Q`` markers must only be detected as standalone tokens.
    Identifiers that legitimately contain ``15`` (UD515) or ``Q`` (UDQ7) must
    NOT trigger the routing flag — exactly as ``_strip_remark_suffixes``
    promises elsewhere in this module.
    """
    assert m._merge_bz_suffix_flags(False, False, "LR6 UD515") == (False, False)
    assert m._merge_bz_suffix_flags(False, False, "LR6 UDQ7") == (False, False)


# --------------------------------------------------------------------------- #
# DMP tray assignment — Bug 1 fix (Request #241 follow-up)
#
# When the frontend filters a composite-remark entry to only the relevant
# production line (e.g. chuyen=501 from "LR6 UD501 UD502"), the backend
# receives entry.groups with only one group.  The fix detects that
# _remark_bz_groups has more entries than entry.groups and uses the full
# remark's group count for positional tray assignment.
# --------------------------------------------------------------------------- #


def _remark_chuyen_to_pos_from_remark(clean_remark: str) -> dict:
    """Helper: build the chuyen→slot index map the Bug 1 fix uses at runtime."""
    remark_groups = m._parse_bz_groups(clean_remark)
    sorted_remark = sorted(
        remark_groups,
        key=lambda rg: (
            (0, int(str(rg.get("chuyen", "") or "0")), str(rg.get("chuyen", "")))
            if str(rg.get("chuyen", "")).isdigit()
            else (1, 0, str(rg.get("chuyen", "")))
        ),
    )
    return {str(rg.get("chuyen", "")): i for i, rg in enumerate(sorted_remark)}


def test_dmp_tray_assignment_composite_remark_chuyen501() -> None:
    """Entry filtered to chuyen=501 from "LR6 UD501 UD502 15":
    chuyen 501 should receive trays 1–4 (slot 0 of the 2-group split), NOT
    all 9 trays which was the bug.
    """
    clean_remark = "LR6 UD501 UD502"  # suffix stripped by _strip_remark_suffixes
    remark_groups = m._parse_bz_groups(clean_remark)
    assert len(remark_groups) == 2, "remark must parse to 2 groups"

    eff_groups = m._sort_eff_groups_for_tray_assignment(
        [{"loai": "UD", "chuyen": "501", "trays": [], "_orig_idx": 0}]
    )
    n_groups = len(eff_groups)  # 1 — filtered entry
    remark_n = len(remark_groups)  # 2 — full remark

    # Without fix: n_groups=1, all 9 trays — demonstrates the bug
    auto_trays_buggy = m._DMP_TRAY_ASSIGNMENT.get(n_groups, [list(range(1, 10))])
    assert auto_trays_buggy == [list(range(1, 10))], "baseline: buggy assigns all 9 trays"

    # With fix: use remark_n=2, find slot for chuyen 501
    remark_pos = _remark_chuyen_to_pos_from_remark(clean_remark)
    auto_trays_fixed = m._split_active_trays_for_group_count(remark_n, list(range(1, 10)))
    dg_pos = remark_pos.get("501", 0)
    trays = auto_trays_fixed[dg_pos] if dg_pos < len(auto_trays_fixed) else []

    assert trays == [1, 2, 3, 4], f"chuyen 501 must map to trays 1-4, got {trays}"


def test_dmp_tray_assignment_composite_remark_chuyen502() -> None:
    """Entry filtered to chuyen=502 from "LR6 UD501 UD502" should receive the
    next 4 active trays (slot 1 of the 2-group split).
    """
    clean_remark = "LR6 UD501 UD502"
    remark_groups = m._parse_bz_groups(clean_remark)
    remark_n = len(remark_groups)  # 2

    remark_pos = _remark_chuyen_to_pos_from_remark(clean_remark)
    auto_trays_fixed = m._split_active_trays_for_group_count(remark_n, list(range(1, 10)))
    dg_pos = remark_pos.get("502", 0)
    trays = auto_trays_fixed[dg_pos] if dg_pos < len(auto_trays_fixed) else []

    assert trays == [5, 6, 7, 8], f"chuyen 502 must map to trays 5-8, got {trays}"


def test_dmp_tray_assignment_two_lines_uses_first_eight_active_trays() -> None:
    """For two-line remarks, empty/broken trays are skipped before assigning
    the first 4 active trays to line 1 and the next 4 active trays to line 2.
    """
    assert m._split_active_trays_for_group_count(
        2, [1, 2, 4, 5, 6, 7, 8, 9]
    ) == [[1, 2, 4, 5], [6, 7, 8, 9]]


def test_dmp_tray_assignment_two_lines_tray7_broken() -> None:
    """Example 2 from the operator spec: tray 7 is damaged and the battery is
    moved to tray 5.  The 4+4 sequential split must give line 501 trays
    [1, 2, 3, 4] and line 502 trays [5, 6, 8, 9].
    """
    assert m._split_active_trays_for_group_count(
        2, [1, 2, 3, 4, 5, 6, 8, 9]
    ) == [[1, 2, 3, 4], [5, 6, 8, 9]]


def test_dmp_tray_assignment_two_lines_all_nine_active_drops_extra_tray() -> None:
    """Special case: when 2 production lines are tested but all 9 trays
    contain valid data, only the first 8 valid trays are used (4 + 4) and
    the 9th is ignored automatically — operators are responsible for
    ensuring each line has exactly 4 batteries.
    """
    assert m._split_active_trays_for_group_count(
        2, [1, 2, 3, 4, 5, 6, 7, 8, 9]
    ) == [[1, 2, 3, 4], [5, 6, 7, 8]]


def test_dmp_tray_assignment_single_line_accepts_any_active_count() -> None:
    """For single-line testing, the operator may place any number of
    batteries on the 9 trays — the tray list is just whatever has data,
    no fixed count is required.
    """
    assert m._split_active_trays_for_group_count(1, [1, 2, 3, 4, 5]) == [
        [1, 2, 3, 4, 5]
    ]
    assert m._split_active_trays_for_group_count(
        1, [1, 2, 3, 4, 5, 6, 7, 8, 9]
    ) == [[1, 2, 3, 4, 5, 6, 7, 8, 9]]
    # Single-line with a broken tray: simply skip the missing tray.
    assert m._split_active_trays_for_group_count(
        1, [1, 2, 4, 5, 6, 7, 8, 9]
    ) == [[1, 2, 4, 5, 6, 7, 8, 9]]


def test_dmp_tray_assignment_single_group_unchanged() -> None:
    """When the remark has only one production-line group, the fix is a no-op
    and all 9 trays are still assigned (correct for single-line batches).
    """
    clean_remark = "LR6 UDP501"
    remark_groups = m._parse_bz_groups(clean_remark)
    remark_n = len(remark_groups)  # 1

    eff_groups = m._sort_eff_groups_for_tray_assignment(
        [{"loai": "UD+", "chuyen": "501", "trays": [], "_orig_idx": 0}]
    )
    n_groups = len(eff_groups)  # 1

    # remark_n == n_groups → fix does not activate
    assert remark_n == n_groups
    auto_trays = m._DMP_TRAY_ASSIGNMENT.get(n_groups, [list(range(1, 10))])
    assert auto_trays == [list(range(1, 10))], "single-group remark must still use all 9 trays"


def test_dmp_tray_assignment_explicit_trays_bypassed() -> None:
    """When entry.groups carries explicit tray lists, the positional assignment
    is bypassed entirely.  The Bug 1 code path must not activate.
    """
    # eff_groups with explicit trays → _sort_eff_groups_for_tray_assignment
    # returns them unchanged because any(g.get("trays")) is True.
    eff_groups = [{"loai": "UD", "chuyen": "501", "trays": [1, 2, 3, 4], "_orig_idx": 0}]
    result = m._sort_eff_groups_for_tray_assignment(eff_groups)
    # The function returns the list unchanged when explicit trays are present
    assert result[0]["trays"] == [1, 2, 3, 4]
    # With explicit trays, _no_explicit_trays would be False → fix skipped
    has_explicit = any(g.get("trays") for g in result)
    assert has_explicit, "explicit trays must be detected"


# --------------------------------------------------------------------------- #
# DMP exact-match batch search — Bug 2 fix (Request #241 follow-up)
#
# The broad ``bz LIKE %clean_remark%`` search picks the most-recent matching
# batch, which is often the wrong one (e.g. "LR6 UD501 UD502" with a later
# fdrq instead of "LR6 UD501 15" for an entry whose raw_remark = "LR6 UD501 15").
# The fix tries ``bz = raw_remark`` first (exact match).  We verify that the
# raw_remark is preserved with its "15"/"Q" suffix, while clean_remark has the
# suffix stripped — the two tokens are exactly what drive the SQL priorities.
# --------------------------------------------------------------------------- #


def test_dmp_exact_match_raw_remark_preserves_15_suffix() -> None:
    """_strip_remark_suffixes must strip "15" / "Q" from the remark (giving
    clean_remark used for the LIKE fallback) while raw_remark keeps the suffix
    (used for the exact-match search that drives Bug 2 fix).
    """
    clean, is_q, is_15d = m._strip_remark_suffixes("LR6 UD501 15")
    assert clean == "LR6 UD501", f"clean_remark should strip '15': got '{clean}'"
    assert is_15d is True
    assert is_q is False
    # raw_remark = "LR6 UD501 15" (unchanged) is the exact bz= query value


def test_dmp_exact_match_raw_remark_preserves_q_suffix() -> None:
    """Same for the Q suffix."""
    clean, is_q, is_15d = m._strip_remark_suffixes("LR6 HP501 HP502 Q")
    assert clean == "LR6 HP501 HP502"
    assert is_q is True
    assert is_15d is False


def test_dmp_exact_match_composite_remark_round_trips() -> None:
    """A composite remark like "LR6 UD501 UD502 15" must also strip correctly
    so that the exact-match query targets the right DMP batch.
    """
    clean, is_q, is_15d = m._strip_remark_suffixes("LR6 UD501 UD502 15")
    assert clean == "LR6 UD501 UD502"
    assert is_15d is True
    # The raw_remark "LR6 UD501 UD502 15" is passed as the exact bz= value;
    # the LIKE fallback uses "LR6 UD501 UD502" which would also match
    # "LR6 UD501 UD502" (non-15D batch) — exactly the wrong batch the fix avoids.


def test_dmp_like_fallback_processes_all_matched_batches(monkeypatch: pytest.MonkeyPatch) -> None:
    """If exact bz matching misses because para_pub.bz lacks the 15 suffix, the
    LIKE fallback may return several LR6 501 batches.  All matched batches must
    become report rows; previously only the first row was processed.
    """
    matched_batches = [
        {
            "id": f"B{i}",
            "dcxh": "LR6",
            "fdrq": f"2026-04-{i:02d}",
            "fdfs": "",
            "jstj": "(1500mW2s,650mW28s)10T/h,24h/d",
            "hfsj": "times",
            "zzdy": "1.05",
            "bz": "LR6 UD501",
        }
        for i in range(1, 4)
    ]

    def fake_read_dmpdata(sql, params=()):
        if "FROM para_pub" in sql and "WHERE bz = ?" in sql:
            return []
        if "FROM para_pub" in sql and "WHERE bz LIKE ?" in sql:
            return matched_batches
        if "FROM para_singl" in sql and "SELECT baty, cdmc" in sql:
            return [{"baty": i, "cdmc": f"tray{i}.mdb"} for i in range(1, 10)]
        if "SELECT scrq FROM para_singl" in sql:
            return []
        return []

    monkeypatch.setattr(m, "_read_dmpdata", fake_read_dmpdata)
    monkeypatch.setattr(
        m,
        "_dmp_compute_group_perf",
        lambda batch_id, trays, endpoint_voltage: {
            "avg_hours": None,
            "avg_minutes": None,
            "avg_count": int(str(batch_id).lstrip("B")),
            "uniform_rate": 100.0,
            "is_dmp": True,
        },
    )

    payload = m.DmpPerfReportRequest(
        entries=[
            m.DmpPerfEntry(
                batch_id="unused",
                model="LR6",
                groups=[m.DmpPerfGroup(loai="UD", chuyen="501", trays=[])],
                raw_remark="LR6 UD501 15",
            )
        ]
    )

    groups = m._compute_dmp_perf_groups(payload)
    rows = groups["LR6 501"]
    assert set(rows) == {
        ("2026-04-01", "UD"),
        ("2026-04-02", "UD"),
        ("2026-04-03", "UD"),
    }
    assert rows[("2026-04-03", "UD")][m._LR6_1500MW_15D_LABEL]["avg_count"] == 3


def test_dmp_row_label_uses_scrq_over_fdrq(monkeypatch: pytest.MonkeyPatch) -> None:
    """para_singl.scrq (manufacture date) must be used as the row label (Date
    column) in View Report instead of para_pub.fdrq (discharge start date).

    Previously, the sid query was run with an int()-cast parameter which Access
    cannot match against a TEXT column, causing scrq lookups to silently return
    0 rows and the code to always fall back to para_pub.fdrq.
    """
    # Simulate a real DMP batch with id '2024073110512202' (16-digit TEXT string),
    # a fdrq discharge date, and a scrq manufacture date that differ so we can
    # tell which one ends up as the row label.
    batch_id = "2024073110512202"
    matched_batches = [
        {
            "id": batch_id,
            "dcxh": "LR6HP",
            "fdrq": "2024-07-31",         # discharge start date — must NOT be used
            "fdfs": "(1500mW2s,650mW28s)10T/h,24h/d",
            "jstj": "(1500mW2s,650mW28s)10T/h,24h/d",
            "hfsj": "times",
            "zzdy": "1.05",
            "bz": "LR6 HP501",
        }
    ]
    # scrq comes from para_singl and differs from fdrq
    scrq_value = "7/15/2024"    # manufacture date in DMP M/D/YYYY format
    scrq_date_str = "2024-07-15"  # expected normalised form

    def fake_read_dmpdata(sql, params=()):
        if "FROM para_pub" in sql and "WHERE bz = ?" in sql:
            return matched_batches
        if "FROM para_singl" in sql and "SELECT baty, cdmc" in sql:
            return [{"baty": i, "cdmc": f"tray{i}.mdb"} for i in range(1, 10)]
        if "SELECT scrq FROM para_singl WHERE sid = ?" in sql:
            # The parameter must be the STRING batch_id, not an integer.
            assert params and isinstance(params[0], str), (
                f"sid query param must be a string, got {type(params[0])}: {params[0]!r}"
            )
            assert params[0] == batch_id
            return [{"scrq": scrq_value}]
        if "SELECT scrq FROM para_singl" in sql:
            return []
        return []

    monkeypatch.setattr(m, "_read_dmpdata", fake_read_dmpdata)
    monkeypatch.setattr(
        m,
        "_dmp_compute_group_perf",
        lambda batch_id, trays, endpoint_voltage: {
            "avg_hours": 8.5,
            "avg_minutes": None,
            "avg_count": None,
            "uniform_rate": 99.0,
            "is_dmp": True,
        },
    )

    payload = m.DmpPerfReportRequest(
        entries=[
            m.DmpPerfEntry(
                batch_id="unused",
                model="LR6",
                groups=[m.DmpPerfGroup(loai="HP", chuyen="501", trays=[])],
                raw_remark="LR6 HP501",
            )
        ]
    )

    groups = m._compute_dmp_perf_groups(payload)
    rows = groups["LR6 501"]
    # Row label must be the scrq manufacture date, not the fdrq discharge date.
    assert (scrq_date_str, "HP") in rows, (
        f"Expected row label '{scrq_date_str}' (scrq) but got keys: {list(rows.keys())}"
    )
    assert ("2024-07-31", "HP") not in rows, (
        "Row label must not be para_pub.fdrq '2024-07-31'; scrq must take precedence"
    )

# --------------------------------------------------------------------------- #
# _parse_access_date — date format normalisation
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("raw, expected", [
    # Standard formats (must remain unchanged)
    ("7/31/2024",      "2024-07-31"),   # M/D/YYYY (DMP para_singl legacy)
    ("07/31/2024",     "2024-07-31"),   # MM/DD/YYYY
    ("1/5/2025",       "2025-01-05"),   # M/D/YYYY single-digit month
    ("2025/3/14",      "2025-03-14"),   # YYYY/M/D (DM2000 / newer DMP)
    ("2026/1/6",       "2026-01-06"),   # YYYY/M/D single-digit month+day
    ("2026-01-06",     "2026-01-06"),   # YYYY-MM-DD
    ("2026-1-6",       "2026-01-06"),   # YYYY-M-D
    # Partial date YYYY/M → 1st of month
    ("2024/10",        "2024-10-01"),   # only year+month, no day
    ("2025/3",         "2025-03-01"),
    ("2026-04",        "2026-04-01"),   # dash separator
    # Range-day notation → start date only
    ("2025/3/15-17",   "2025-03-15"),   # day range within month
    ("2025/03/20-21",  "2025-03-20"),
    ("2025/4/30-1/5",  None),           # cross-month range spans 4 slash-parts → unparseable
    ("2025/3/29-31",   "2025-03-29"),
    # Compound suffix "29-31-1": observed in production dmpdata.mdb (para_singl.scrq).
    # The day component "29-31-1" is interpreted as start-day 29, rest is discarded.
    ("2025/3/29-31-1", "2025-03-29"),
    # Garbage / unparseable → None
    ("20225/6/9",      None),           # typo year
    ("None",           None),
    ("",               None),
    ("19/2-25/2",      None),           # no-year range
])
def test_parse_access_date(raw, expected):
    """_parse_access_date must normalise all Access date string formats to YYYY-MM-DD."""
    assert m._parse_access_date(raw) == expected, f"_parse_access_date({raw!r}) should be {expected!r}"


def test_dmp_row_label_scrq_range_day(monkeypatch: pytest.MonkeyPatch) -> None:
    """When para_singl.scrq uses range-day notation (e.g. '2025/3/15-17') the
    View Report row label must be the start date ('2025-03-15'), NOT fdrq.
    """
    batch_id = "2025031510000001"
    fdrq_value = "2025-04-01"           # discharge date — must NOT appear as row label
    scrq_value = "2025/3/15-17"         # range notation manufacture date
    scrq_start_date = "2025-03-15"      # expected row label

    matched_batches = [{
        "id": batch_id,
        "dcxh": "LR6HP",
        "fdrq": fdrq_value,
        "fdfs": "(1500mW2s,650mW28s)10T/h,24h/d",
        "jstj": "(1500mW2s,650mW28s)10T/h,24h/d",
        "hfsj": "times",
        "zzdy": "1.05",
        "bz": "LR6 HP999",
    }]

    def fake_read_dmpdata(sql, params=()):
        if "FROM para_pub" in sql and "WHERE bz = ?" in sql:
            return matched_batches
        if "FROM para_singl" in sql and "SELECT baty, cdmc" in sql:
            return [{"baty": i, "cdmc": f"tray{i}.mdb"} for i in range(1, 10)]
        if "SELECT scrq FROM para_singl WHERE sid = ?" in sql:
            return [{"scrq": scrq_value}]
        if "SELECT scrq FROM para_singl" in sql:
            return []
        return []

    monkeypatch.setattr(m, "_read_dmpdata", fake_read_dmpdata)
    monkeypatch.setattr(
        m,
        "_dmp_compute_group_perf",
        lambda batch_id, trays, endpoint_voltage: {
            "avg_hours": 8.0,
            "avg_minutes": None,
            "avg_count": None,
            "uniform_rate": 98.0,
            "is_dmp": True,
        },
    )

    payload = m.DmpPerfReportRequest(
        entries=[
            m.DmpPerfEntry(
                batch_id="unused",
                model="LR6",
                groups=[m.DmpPerfGroup(loai="HP", chuyen="999", trays=[])],
                raw_remark="LR6 HP999",
            )
        ]
    )

    groups = m._compute_dmp_perf_groups(payload)
    rows = groups["LR6 999"]
    assert (scrq_start_date, "HP") in rows, (
        f"Expected row label '{scrq_start_date}' (scrq start) but got: {list(rows.keys())}"
    )
    assert (fdrq_value, "HP") not in rows, (
        f"Row label must not be fdrq '{fdrq_value}'; scrq range start must take precedence"
    )


def test_dmp_row_label_scrq_partial_date(monkeypatch: pytest.MonkeyPatch) -> None:
    """When para_singl.scrq is a partial 'YYYY/M' date the View Report row
    label must be the 1st of that month, NOT fdrq.
    """
    batch_id = "2024100100000001"
    fdrq_value = "2024-11-01"
    scrq_value = "2024/10"              # year+month only, no day
    scrq_parsed_date = "2024-10-01"     # expected row label: 1st of the month

    matched_batches = [{
        "id": batch_id,
        "dcxh": "LR6HP",
        "fdrq": fdrq_value,
        "fdfs": "(1500mW2s,650mW28s)10T/h,24h/d",
        "jstj": "(1500mW2s,650mW28s)10T/h,24h/d",
        "hfsj": "times",
        "zzdy": "1.05",
        "bz": "LR6 HP888",
    }]

    def fake_read_dmpdata(sql, params=()):
        if "FROM para_pub" in sql and "WHERE bz = ?" in sql:
            return matched_batches
        if "FROM para_singl" in sql and "SELECT baty, cdmc" in sql:
            return [{"baty": i, "cdmc": f"tray{i}.mdb"} for i in range(1, 10)]
        if "SELECT scrq FROM para_singl WHERE sid = ?" in sql:
            return [{"scrq": scrq_value}]
        if "SELECT scrq FROM para_singl" in sql:
            return []
        return []

    monkeypatch.setattr(m, "_read_dmpdata", fake_read_dmpdata)
    monkeypatch.setattr(
        m,
        "_dmp_compute_group_perf",
        lambda batch_id, trays, endpoint_voltage: {
            "avg_hours": 7.5,
            "avg_minutes": None,
            "avg_count": None,
            "uniform_rate": 97.0,
            "is_dmp": True,
        },
    )

    payload = m.DmpPerfReportRequest(
        entries=[
            m.DmpPerfEntry(
                batch_id="unused",
                model="LR6",
                groups=[m.DmpPerfGroup(loai="HP", chuyen="888", trays=[])],
                raw_remark="LR6 HP888",
            )
        ]
    )

    groups = m._compute_dmp_perf_groups(payload)
    rows = groups["LR6 888"]
    assert (scrq_parsed_date, "HP") in rows, (
        f"Expected row label '{scrq_parsed_date}' (scrq 1st of month) but got: {list(rows.keys())}"
    )
    assert (fdrq_value, "HP") not in rows, (
        f"Row label must not be fdrq '{fdrq_value}'; scrq partial date must take precedence"
    )


# --------------------------------------------------------------------------- #
# Request #246 follow-up: the matched batch's bz is the canonical source of
# multi-line structure for tray-positional assignment.
#
# The user reported that two-line LR6 (1500mW2s,650mW28s)10T/h,24h/d batches
# (with or without the "15" suffix) load incorrectly: one production line
# averages all 9 trays while the other has no data.  Investigation shows that
# the existing positional-split fix uses the entry's raw_remark group count.
# When the entry's raw_remark mentions only its own line (e.g. "LR6 UD501")
# but the matched para_pub.bz is the multi-line composite ("LR6 UD501 UD502"),
# the fix does not activate and the single group falls through to the all-9-
# trays default.  The matched batch's bz (the operator-edited master record)
# must be the canonical source of multi-line structure for ALL bz patterns.
# --------------------------------------------------------------------------- #


def _make_dmp_batch(
    *,
    batch_id: str,
    bz: str,
    fdrq: str = "2026-04-18",
    jstj: str = "(1500mW2s,650mW28s)10T/h,24h/d",
    zzdy: str = "1.05",
) -> dict:
    return {
        "id": batch_id,
        "dcxh": "LR6",
        "fdrq": fdrq,
        "fdfs": "",
        "jstj": jstj,
        "hfsj": "times",
        "zzdy": zzdy,
        "bz": bz,
    }


def _install_dmp_batch_fakes(
    monkeypatch: pytest.MonkeyPatch,
    *,
    exact_batches: list[dict] | None = None,
    like_batches: list[dict] | None = None,
    active_trays: list[int] | None = None,
    active_trays_by_batch: dict[str, list[int]] | None = None,
    scdw_by_batch: dict[str, str] | None = None,
) -> dict:
    """Install fake _read_dmpdata + _dmp_compute_group_perf for batch tests.

    Returns a captured dict that records every (batch_id, trays) call to
    _dmp_compute_group_perf so the test can assert which trays each
    production-line group was actually computed over.

    *active_trays_by_batch* lets a test give each batch its own active-tray
    list (required when one bz LIKE match returns several batches whose
    physical tray populations differ — e.g. one batch with trays 1-4
    populated and a sibling batch with trays 6-9 populated).  Falls back to
    *active_trays* (default 1..9) for batch ids not present in the map.

    *scdw_by_batch* attaches a per-batch ``para_singl.scdw`` value (the
    production-line tag, e.g. ``"VN501"``).  Used by the perf-report writer
    to route data from single-line batches to the correct line group when
    they share a multi-line ``bz`` remark with a sibling batch.
    """
    if active_trays is None:
        active_trays = list(range(1, 10))
    if active_trays_by_batch is None:
        active_trays_by_batch = {}
    if scdw_by_batch is None:
        scdw_by_batch = {}
    captured: dict[tuple[str, str], list[int]] = {}

    def fake_read_dmpdata(sql, params=()):
        if "FROM para_pub" in sql and "WHERE bz = ?" in sql:
            return list(exact_batches or [])
        if "FROM para_pub" in sql and "WHERE bz LIKE ?" in sql:
            return list(like_batches or [])
        if "FROM para_singl" in sql and "SELECT baty, cdmc" in sql:
            sid = str(params[0]) if params else ""
            trays = active_trays_by_batch.get(sid, active_trays)
            return [
                {"baty": i, "cdmc": f"sub_{sid}.mdb"} for i in trays
            ]
        if "SELECT scdw FROM para_singl" in sql:
            sid = str(params[0]) if params else ""
            scdw = scdw_by_batch.get(sid, "")
            return [{"scdw": scdw}] if scdw else []
        if "SELECT scrq FROM para_singl" in sql:
            return []
        return []

    def fake_compute(batch_id, trays, endpoint_voltage):
        # Record by batch_id; tests then verify the trays per line.
        # Use a synthetic key (batch_id, sorted-trays-str) so multiple groups
        # on the same batch are distinct entries.
        captured[(str(batch_id), ",".join(str(t) for t in trays))] = list(trays)
        return {
            "avg_hours": float(len(trays)),
            "avg_minutes": None,
            "avg_count": len(trays),
            "uniform_rate": 100.0,
            "is_dmp": True,
        }

    monkeypatch.setattr(m, "_read_dmpdata", fake_read_dmpdata)
    monkeypatch.setattr(m, "_dmp_compute_group_perf", fake_compute)
    return captured


def test_dmp_canonical_split_uses_batch_bz_when_entry_remark_is_partial(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """User-reported bug (Request #246 follow-up).

    Entry has raw_remark="LR6 UD501" (single line) but the matched para_pub.bz
    master record is the composite "LR6 UD501 UD502" (two lines).  The
    positional tray split must use the BATCH's bz to derive the line count
    so that chuyen 501 → trays 1-4, NOT all 9 trays averaged together.
    """
    batch = _make_dmp_batch(
        batch_id="2026041814114611", bz="LR6 UD501 UD502"
    )
    captured = _install_dmp_batch_fakes(
        monkeypatch, exact_batches=[], like_batches=[batch]
    )

    payload = m.DmpPerfReportRequest(
        entries=[
            m.DmpPerfEntry(
                batch_id="unused",
                model="LR6",
                groups=[m.DmpPerfGroup(loai="UD", chuyen="501", trays=[])],
                raw_remark="LR6 UD501",
            )
        ]
    )

    m._compute_dmp_perf_groups(payload)
    # The single chuyen-501 group must be computed over trays 1-4 (slot 0
    # of the 2-line positional split derived from the batch bz), not over
    # all 9 active trays.
    trays_used = [
        v for (bid, _), v in captured.items() if bid == "2026041814114611"
    ]
    assert trays_used == [[1, 2, 3, 4]], (
        f"Expected chuyen 501 to use trays [1,2,3,4] derived from batch "
        f"bz='LR6 UD501 UD502'; got {trays_used}"
    )


def test_dmp_canonical_split_uses_batch_bz_with_15_suffix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same scenario but with the 15-day suffix on both entry and batch bz.

    Entry: raw_remark="LR6 UD501 15" (single line, 15D), batch bz=
    "LR6 UD501 UD502 15" (two lines, 15D).  The chuyen-501 group must take
    the first 4 trays and the result must be routed to the 15D column only.
    """
    batch = _make_dmp_batch(
        batch_id="2026041814120812", bz="LR6 UD501 UD502 15"
    )
    captured = _install_dmp_batch_fakes(
        monkeypatch, exact_batches=[], like_batches=[batch]
    )

    payload = m.DmpPerfReportRequest(
        entries=[
            m.DmpPerfEntry(
                batch_id="unused",
                model="LR6",
                groups=[m.DmpPerfGroup(loai="UD", chuyen="501", trays=[])],
                raw_remark="LR6 UD501 15",
            )
        ]
    )

    groups = m._compute_dmp_perf_groups(payload)
    trays_used = [
        v for (bid, _), v in captured.items() if bid == "2026041814120812"
    ]
    assert trays_used == [[1, 2, 3, 4]], (
        f"With 15 suffix, chuyen 501 must still use [1,2,3,4] from batch "
        f"bz='LR6 UD501 UD502 15'; got {trays_used}"
    )
    # Result must land in the 15D column only (Request #237/#241 invariant).
    rows = groups["LR6 501"]
    assert len(rows) == 1, f"expected exactly one row key, got {list(rows.keys())}"
    only_key = next(iter(rows.keys()))
    assert m._LR6_1500MW_15D_LABEL in rows[only_key]
    assert m._LR6_1500MW_DAILY_LABEL not in rows[only_key]


def test_dmp_canonical_split_chuyen_502_gets_second_slot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mirror test: a separate entry for chuyen 502 (raw_remark "LR6 UD502")
    against the same composite batch bz="LR6 UD501 UD502" must take the
    SECOND positional slot (trays 5-8), not the all-9-trays default.
    """
    batch = _make_dmp_batch(
        batch_id="2026041814114611", bz="LR6 UD501 UD502"
    )
    captured = _install_dmp_batch_fakes(
        monkeypatch, exact_batches=[], like_batches=[batch]
    )

    payload = m.DmpPerfReportRequest(
        entries=[
            m.DmpPerfEntry(
                batch_id="unused",
                model="LR6",
                groups=[m.DmpPerfGroup(loai="UD", chuyen="502", trays=[])],
                raw_remark="LR6 UD502",
            )
        ]
    )

    m._compute_dmp_perf_groups(payload)
    trays_used = [
        v for (bid, _), v in captured.items() if bid == "2026041814114611"
    ]
    assert trays_used == [[5, 6, 7, 8]], (
        f"chuyen 502 must take slot 1 (trays 5-8) of the 2-line split; got {trays_used}"
    )


def test_dmp_canonical_split_three_line_bz(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Generic three-line case: batch bz="LR6 UD501 UD502 UD503" with an
    entry that mentions only chuyen 502 must take the middle slot (trays
    4-6) of the 3+3+3 positional split.
    """
    batch = _make_dmp_batch(
        batch_id="B3LINE", bz="LR6 UD501 UD502 UD503"
    )
    captured = _install_dmp_batch_fakes(
        monkeypatch, exact_batches=[], like_batches=[batch]
    )

    payload = m.DmpPerfReportRequest(
        entries=[
            m.DmpPerfEntry(
                batch_id="unused",
                model="LR6",
                groups=[m.DmpPerfGroup(loai="UD", chuyen="502", trays=[])],
                raw_remark="LR6 UD502",
            )
        ]
    )

    m._compute_dmp_perf_groups(payload)
    trays_used = [v for (bid, _), v in captured.items() if bid == "B3LINE"]
    assert trays_used == [[4, 5, 6]], (
        f"chuyen 502 in a 3-line bz must take middle slot [4,5,6]; got {trays_used}"
    )


def test_dmp_canonical_split_mixed_grades_uses_batch_bz_loai(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the entry's raw_remark uses a different battery grade than the
    batch bz (e.g. entry says "LR6 UD501" but bz says "LR6 UDP501 HP503"),
    the loai mapping must follow the batch bz (the master record).  This
    ensures the row_key uses the correct grade when the entry was created
    with a stale or partial remark.
    """
    batch = _make_dmp_batch(batch_id="BMIXED", bz="LR6 UDP501 HP503")
    captured = _install_dmp_batch_fakes(
        monkeypatch, exact_batches=[], like_batches=[batch]
    )

    payload = m.DmpPerfReportRequest(
        entries=[
            m.DmpPerfEntry(
                batch_id="unused",
                model="LR6",
                groups=[m.DmpPerfGroup(loai="UD", chuyen="503", trays=[])],
                raw_remark="LR6 UD503",
            )
        ]
    )

    groups = m._compute_dmp_perf_groups(payload)
    trays_used = [v for (bid, _), v in captured.items() if bid == "BMIXED"]
    # chuyen 503 is the SECOND group in the batch bz (501 → slot 0, 503 → slot 1)
    assert trays_used == [[5, 6, 7, 8]], (
        f"chuyen 503 (slot 1) must take trays [5-8]; got {trays_used}"
    )
    # loai for chuyen 503 in the batch bz is "HP" — row_key must reflect that
    rows = groups["LR6 503"]
    assert any(loai == "HP" for (_, loai) in rows.keys()), (
        f"row_key loai must come from batch bz (HP for chuyen 503); got {list(rows.keys())}"
    )


def test_dmp_canonical_split_extra_batch_uses_its_own_bz(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Each batch processed in the multi-batch loop has its own bz; the
    positional split must be computed per-batch from each batch's own bz so
    that batches with different multi-line shapes are split correctly.

    Setup: entry raw_remark "LR6 UD501", two matched batches:
      • batch A bz = "LR6 UD501 UD502" (two-line composite) → 501 → trays 1-4
      • batch B bz = "LR6 UD501"       (single-line)        → 501 → trays 1-9
    """
    batch_a = _make_dmp_batch(
        batch_id="BA", bz="LR6 UD501 UD502", fdrq="2026-04-18"
    )
    batch_b = _make_dmp_batch(
        batch_id="BB", bz="LR6 UD501", fdrq="2026-04-17"
    )
    captured = _install_dmp_batch_fakes(
        monkeypatch, exact_batches=[], like_batches=[batch_a, batch_b]
    )

    payload = m.DmpPerfReportRequest(
        entries=[
            m.DmpPerfEntry(
                batch_id="unused",
                model="LR6",
                groups=[m.DmpPerfGroup(loai="UD", chuyen="501", trays=[])],
                raw_remark="LR6 UD501",
            )
        ]
    )

    m._compute_dmp_perf_groups(payload)
    # batch_a has 2-line bz → chuyen 501 takes [1,2,3,4]
    trays_a = [v for (bid, _), v in captured.items() if bid == "BA"]
    assert trays_a == [[1, 2, 3, 4]], (
        f"Composite batch A: chuyen 501 must take [1-4]; got {trays_a}"
    )
    # batch_b has single-line bz → chuyen 501 takes all active trays
    trays_b = [v for (bid, _), v in captured.items() if bid == "BB"]
    assert trays_b == [[1, 2, 3, 4, 5, 6, 7, 8, 9]], (
        f"Single-line batch B: chuyen 501 must take all 9 trays; got {trays_b}"
    )


def test_dmp_canonical_split_two_group_entry_unaffected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the entry already carries both groups (501 and 502), positional
    assignment continues to work as before — the canonical-bz logic must
    not regress the existing two-group entry case.
    """
    batch = _make_dmp_batch(batch_id="B2G", bz="LR6 UD501 UD502")
    captured = _install_dmp_batch_fakes(
        monkeypatch, exact_batches=[], like_batches=[batch]
    )

    payload = m.DmpPerfReportRequest(
        entries=[
            m.DmpPerfEntry(
                batch_id="unused",
                model="LR6",
                groups=[
                    m.DmpPerfGroup(loai="UD", chuyen="501", trays=[]),
                    m.DmpPerfGroup(loai="UD", chuyen="502", trays=[]),
                ],
                raw_remark="LR6 UD501 UD502",
            )
        ]
    )

    m._compute_dmp_perf_groups(payload)
    trays = sorted(v for (bid, _), v in captured.items() if bid == "B2G")
    assert trays == [[1, 2, 3, 4], [5, 6, 7, 8]], (
        f"Two-group entry must split [1-4]/[5-8] as before; got {trays}"
    )


def test_dmp_canonical_split_explicit_trays_bypass_canonical_logic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the entry carries explicit trays, the canonical-bz logic must
    NOT override them — explicit operator configuration always wins.
    """
    batch = _make_dmp_batch(batch_id="BEXP", bz="LR6 UD501 UD502")
    captured = _install_dmp_batch_fakes(
        monkeypatch, exact_batches=[], like_batches=[batch]
    )

    payload = m.DmpPerfReportRequest(
        entries=[
            m.DmpPerfEntry(
                batch_id="unused",
                model="LR6",
                groups=[m.DmpPerfGroup(loai="UD", chuyen="501", trays=[2, 4, 6])],
                raw_remark="LR6 UD501",
            )
        ]
    )

    m._compute_dmp_perf_groups(payload)
    trays = [v for (bid, _), v in captured.items() if bid == "BEXP"]
    assert trays == [[2, 4, 6]], (
        f"Explicit trays must be preserved verbatim; got {trays}"
    )


# --------------------------------------------------------------------------- #
# Per-batch scdw routing
# --------------------------------------------------------------------------- #
# When a single bz LIKE search returns multiple para_pub batches and each
# carries data for only ONE of the lines listed in bz (e.g. the DMP machine
# creates one batch per physical production line, both tagged with the
# composite remark "LR6 UD501 UD502 15"), the per-batch positional split
# must NOT route the batch's data sequentially to both lines based on bz
# token order — the actual production line is encoded in para_singl.scdw
# ("VN501", "VN502", …).  Without this routing, swapping the bz token order
# (e.g. "UD501 UD502" → "UD502 UD501") visibly swapped the resulting
# values, making the assignment dependent on remark order rather than the
# real production-line mapping.
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "scdw,expected",
    [
        ("VN501", ["501"]),
        ("VN 502", ["502"]),
        ("VN501-502", ["501", "502"]),
        ("VN 502-501", ["502", "501"]),
        ("VN501-502-503", ["501", "502", "503"]),
        ("503-504", ["503", "504"]),
        ("VN", []),
        ("", []),
        (None, []),
        # Strip false-positive shorter numerics like a "15" suffix that
        # might accidentally appear in scdw — only ≥3-digit tokens count.
        ("VN501 15", ["501"]),
    ],
)
def test_parse_scdw_chuyens(scdw, expected) -> None:
    assert m._parse_scdw_chuyens(scdw) == expected


def test_dmp_scdw_routes_two_single_line_batches_to_correct_groups(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """User-reported bug.

    Two para_pub batches share bz="LR6 UD501 UD502 15" — one tagged
    scdw="VN501" (data for line 501 only), the other scdw="VN502"
    (data for line 502 only).  Each batch's active trays must be routed
    entirely to ITS production line (not split positionally across both
    lines as the previous logic did, which mis-labelled half of every
    batch and made the result depend on bz token order).
    """
    batch_a = _make_dmp_batch(batch_id="BA", bz="LR6 UD501 UD502 15")
    batch_b = _make_dmp_batch(batch_id="BB", bz="LR6 UD501 UD502 15")
    captured = _install_dmp_batch_fakes(
        monkeypatch,
        exact_batches=[batch_a, batch_b],
        like_batches=[],
        active_trays_by_batch={
            "BA": [1, 2, 3, 4],   # only physical trays 1-4 populated
            "BB": [6, 7, 8, 9],   # only physical trays 6-9 populated
        },
        scdw_by_batch={
            "BA": "VN501",        # batch A is line 501 only
            "BB": "VN502",        # batch B is line 502 only
        },
    )

    payload = m.DmpPerfReportRequest(
        entries=[
            m.DmpPerfEntry(
                batch_id="unused",
                model="LR6",
                groups=[
                    m.DmpPerfGroup(loai="UD", chuyen="501", trays=[]),
                    m.DmpPerfGroup(loai="UD", chuyen="502", trays=[]),
                ],
                raw_remark="LR6 UD501 UD502 15",
            )
        ]
    )

    m._compute_dmp_perf_groups(payload)

    # Batch A (scdw=VN501): all of its active trays must go to chuyen 501,
    # nothing for chuyen 502 (otherwise 501 data would be mis-labelled 502).
    a_trays = sorted(v for (bid, _), v in captured.items() if bid == "BA")
    assert a_trays == [[1, 2, 3, 4]], (
        f"batch A (scdw=VN501) must route all 4 active trays to chuyen 501 "
        f"and nothing to chuyen 502; got {a_trays}"
    )

    # Batch B (scdw=VN502): symmetric.
    b_trays = sorted(v for (bid, _), v in captured.items() if bid == "BB")
    assert b_trays == [[6, 7, 8, 9]], (
        f"batch B (scdw=VN502) must route all 4 active trays to chuyen 502 "
        f"and nothing to chuyen 501; got {b_trays}"
    )


def test_dmp_scdw_routing_independent_of_bz_token_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reversing the bz token order from "UD501 UD502" to "UD502 UD501"
    must NOT swap the resulting line-to-data mapping.  With per-batch
    scdw routing, each batch's data is determined by its scdw tag, not by
    where its line happens to appear in the (operator-edited) bz string.
    """
    batch_a = _make_dmp_batch(batch_id="BA", bz="LR6 UD502 UD501 15")
    batch_b = _make_dmp_batch(batch_id="BB", bz="LR6 UD502 UD501 15")
    captured = _install_dmp_batch_fakes(
        monkeypatch,
        exact_batches=[batch_a, batch_b],
        like_batches=[],
        active_trays_by_batch={
            "BA": [1, 2, 3, 4],
            "BB": [6, 7, 8, 9],
        },
        scdw_by_batch={"BA": "VN501", "BB": "VN502"},
    )

    payload = m.DmpPerfReportRequest(
        entries=[
            m.DmpPerfEntry(
                batch_id="unused",
                model="LR6",
                groups=[
                    m.DmpPerfGroup(loai="UD", chuyen="502", trays=[]),
                    m.DmpPerfGroup(loai="UD", chuyen="501", trays=[]),
                ],
                # raw_remark mirrors the reversed bz (frontend parses
                # groups in remark order).
                raw_remark="LR6 UD502 UD501 15",
            )
        ]
    )

    m._compute_dmp_perf_groups(payload)

    # Same expectation as the non-reversed case: line 501 owns batch A's
    # trays, line 502 owns batch B's trays.
    a_trays = sorted(v for (bid, _), v in captured.items() if bid == "BA")
    b_trays = sorted(v for (bid, _), v in captured.items() if bid == "BB")
    assert a_trays == [[1, 2, 3, 4]], (
        f"reversed bz must NOT swap routing — batch A (scdw=VN501) still "
        f"owns trays 1-4 for chuyen 501; got {a_trays}"
    )
    assert b_trays == [[6, 7, 8, 9]], (
        f"reversed bz must NOT swap routing — batch B (scdw=VN502) still "
        f"owns trays 6-9 for chuyen 502; got {b_trays}"
    )


def test_dmp_scdw_single_line_batch_partial_entry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the entry has only one group (e.g. just chuyen=502) and the
    batch is tagged scdw="VN502", the batch's active trays must all go to
    the entry's group regardless of the bz containing extra lines.
    Conversely, a batch tagged scdw="VN501" must contribute nothing to a
    chuyen-502 entry (sentinel routing → empty trays → skipped).
    """
    batch_other = _make_dmp_batch(batch_id="BOTHER", bz="LR6 UD501 UD502 15")
    batch_target = _make_dmp_batch(batch_id="BTARGET", bz="LR6 UD501 UD502 15")
    captured = _install_dmp_batch_fakes(
        monkeypatch,
        exact_batches=[batch_other, batch_target],
        like_batches=[],
        active_trays_by_batch={
            "BOTHER": [1, 2, 3, 4],
            "BTARGET": [6, 7, 8, 9],
        },
        scdw_by_batch={"BOTHER": "VN501", "BTARGET": "VN502"},
    )

    payload = m.DmpPerfReportRequest(
        entries=[
            m.DmpPerfEntry(
                batch_id="unused",
                model="LR6",
                groups=[m.DmpPerfGroup(loai="UD", chuyen="502", trays=[])],
                raw_remark="LR6 UD501 UD502 15",
            )
        ]
    )

    m._compute_dmp_perf_groups(payload)

    # The "wrong-line" batch (scdw=VN501) must not contribute trays for the
    # chuyen-502 entry.  No captured call for BOTHER means the routing
    # correctly returned [] and the per-batch loop short-circuited via
    # ``if not trays: continue``.
    assert all(bid != "BOTHER" for (bid, _) in captured), (
        f"chuyen-502 entry must receive NO trays from batch BOTHER "
        f"(scdw=VN501); got captured calls {list(captured)}"
    )
    # The matching batch (scdw=VN502) must contribute all its active trays.
    target_trays = [v for (bid, _), v in captured.items() if bid == "BTARGET"]
    assert target_trays == [[6, 7, 8, 9]], (
        f"chuyen-502 entry must use ALL of batch BTARGET's active trays "
        f"(scdw=VN502 narrows the routing); got {target_trays}"
    )


def test_dmp_scdw_equal_to_bz_keeps_positional_split(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When scdw covers exactly the same lines as the bz canonical chuyens
    (e.g. scdw="VN501-502" with bz="LR6 UD501 UD502"), the batch genuinely
    contains data for both lines — the existing positional 4+4 split must
    be used unchanged so this fix does not regress the well-behaved case.
    """
    batch = _make_dmp_batch(batch_id="BFULL", bz="LR6 UD501 UD502")
    captured = _install_dmp_batch_fakes(
        monkeypatch,
        exact_batches=[],
        like_batches=[batch],
        scdw_by_batch={"BFULL": "VN501-502"},
    )

    payload = m.DmpPerfReportRequest(
        entries=[
            m.DmpPerfEntry(
                batch_id="unused",
                model="LR6",
                groups=[m.DmpPerfGroup(loai="UD", chuyen="501", trays=[])],
                raw_remark="LR6 UD501",
            )
        ]
    )

    m._compute_dmp_perf_groups(payload)
    trays = [v for (bid, _), v in captured.items() if bid == "BFULL"]
    # scdw equals the canonical chuyens → no narrowing → preserve existing
    # behaviour: chuyen 501 still gets slot 0 of the 2-line positional
    # split (trays 1-4) just as before this fix.
    assert trays == [[1, 2, 3, 4]], (
        f"scdw equal to bz must preserve positional split; got {trays}"
    )


def test_dmp_scdw_unparseable_keeps_positional_split(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When scdw cannot be parsed (empty/garbage), fall back to the
    existing canonical positional split — the fix is strictly additive.
    """
    batch = _make_dmp_batch(batch_id="BNOSCDW", bz="LR6 UD501 UD502")
    captured = _install_dmp_batch_fakes(
        monkeypatch,
        exact_batches=[],
        like_batches=[batch],
        # No scdw entry → fake returns no rows → _parse_scdw_chuyens([]) → []
    )

    payload = m.DmpPerfReportRequest(
        entries=[
            m.DmpPerfEntry(
                batch_id="unused",
                model="LR6",
                groups=[m.DmpPerfGroup(loai="UD", chuyen="502", trays=[])],
                raw_remark="LR6 UD502",
            )
        ]
    )

    m._compute_dmp_perf_groups(payload)
    trays = [v for (bid, _), v in captured.items() if bid == "BNOSCDW"]
    # Without scdw narrowing, chuyen 502 takes slot 1 (trays 5-8) of the
    # 2-line positional split derived from bz — same as Request #246.
    assert trays == [[5, 6, 7, 8]], (
        f"missing scdw must fall back to positional split; got {trays}"
    )


# --------------------------------------------------------------------------- #
# Per-batch scdw routing — disjoint and partial-overlap cases (Request #248
# follow-up).  PR #248 only narrowed routing when ``scdw`` was a *strict
# subset* of the canonical chuyens, which left two important cases broken:
#
#   * scdw disjoint from canonical (e.g. scdw="503-504" while bz canonical
#     is {501,502}) — PR #248 fell through to the positional 4+4 split, so
#     the 503/504 batch's data silently contaminated both the 501 and 502
#     rows.  Real production data: a batch with bz="LR6 UD501 UD502 15"
#     can carry scdw="503-504" because the bz field is operator-edited
#     free text and does not always match the physical production line.
#   * partial overlap (e.g. scdw="502-503" with canonical {501,502}) —
#     same fall-through, mis-routing 502's data onto 501's row.
# --------------------------------------------------------------------------- #


def test_dmp_scdw_disjoint_from_canonical_skips_batch_entirely(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A batch tagged with a production line not listed in the entry's
    canonical chuyens (e.g. scdw="503-504" against bz="LR6 UD501 UD502 15")
    must contribute NO data to either chuyen 501 or 502 — its physical
    batteries belong to the 503/504 line.  Without this rule the
    positional split silently mis-labels 503/504 data as 501/502.
    """
    batch_good = _make_dmp_batch(batch_id="BGOOD", bz="LR6 UD501 UD502 15")
    batch_wrong = _make_dmp_batch(batch_id="BWRONG", bz="LR6 UD501 UD502 15")
    captured = _install_dmp_batch_fakes(
        monkeypatch,
        exact_batches=[batch_good, batch_wrong],
        like_batches=[],
        active_trays_by_batch={
            "BGOOD": [1, 2, 3, 4, 5, 6, 7, 8, 9],
            "BWRONG": [1, 2, 3, 4, 5, 6, 7, 8, 9],
        },
        scdw_by_batch={
            "BGOOD": "VN501-502",   # really lines 501+502
            "BWRONG": "503-504",    # mis-labelled bz; data is for 503-504
        },
    )

    payload = m.DmpPerfReportRequest(
        entries=[
            m.DmpPerfEntry(
                batch_id="unused",
                model="LR6",
                groups=[
                    m.DmpPerfGroup(loai="UD", chuyen="501", trays=[]),
                    m.DmpPerfGroup(loai="UD", chuyen="502", trays=[]),
                ],
                raw_remark="LR6 UD501 UD502 15",
            )
        ]
    )

    m._compute_dmp_perf_groups(payload)

    # The "wrong-line" batch must not have been read for any chuyen of
    # this entry — its scdw lines (503/504) do not intersect the entry's
    # canonical chuyens (501/502).
    assert all(bid != "BWRONG" for (bid, _) in captured), (
        f"batch with disjoint scdw must contribute NO trays to either "
        f"line of the entry; got captured calls {list(captured)}"
    )

    # The well-matched batch (scdw == canonical) keeps the positional
    # 4+4 split: 501 → trays 1-4, 502 → trays 5-8.
    good_trays = sorted(v for (bid, _), v in captured.items() if bid == "BGOOD")
    assert good_trays == [[1, 2, 3, 4], [5, 6, 7, 8]], (
        f"matching batch must use positional 4+4 split; got {good_trays}"
    )


def test_dmp_scdw_partial_overlap_routes_only_intersection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When scdw partially overlaps the canonical chuyens (e.g.
    scdw="502-503" against bz="LR6 UD501 UD502"), the batch must
    contribute only to the intersecting line (502) and skip the others
    — never feeding its 503-line data into the 501 row via the
    positional 4+4 split.
    """
    batch = _make_dmp_batch(batch_id="BMIX", bz="LR6 UD501 UD502")
    captured = _install_dmp_batch_fakes(
        monkeypatch,
        exact_batches=[],
        like_batches=[batch],
        active_trays_by_batch={"BMIX": [1, 2, 3, 4, 5, 6, 7, 8, 9]},
        scdw_by_batch={"BMIX": "502-503"},
    )

    payload = m.DmpPerfReportRequest(
        entries=[
            m.DmpPerfEntry(
                batch_id="unused",
                model="LR6",
                groups=[
                    m.DmpPerfGroup(loai="UD", chuyen="501", trays=[]),
                    m.DmpPerfGroup(loai="UD", chuyen="502", trays=[]),
                ],
                raw_remark="LR6 UD501 UD502",
            )
        ]
    )

    m._compute_dmp_perf_groups(payload)

    captured_trays = sorted(v for (bid, _), v in captured.items() if bid == "BMIX")
    # intersection = {502} → single slot covers all 9 active trays for
    # the chuyen-502 group; chuyen 501 is marked absent → no call.
    assert captured_trays == [[1, 2, 3, 4, 5, 6, 7, 8, 9]], (
        f"partial-overlap scdw must route ALL active trays to the "
        f"intersecting line (502) and skip the non-intersecting line "
        f"(501); got {captured_trays}"
    )


def test_dmp_scdw_disjoint_routing_independent_of_remark_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reversing the entry remark from "LR6 UD501 UD502 15" to
    "LR6 UD502 UD501 15" must not change which line gets which batch's
    data — the line-to-data mapping is determined by scdw and chuyen
    number, never by the position of a token in the remark string.
    """
    batch_good = _make_dmp_batch(batch_id="BGOOD", bz="LR6 UD501 UD502 15")
    batch_wrong = _make_dmp_batch(batch_id="BWRONG", bz="LR6 UD501 UD502 15")

    def _run(raw_remark: str, group_order: list[str]) -> dict:
        captured = _install_dmp_batch_fakes(
            monkeypatch,
            exact_batches=[batch_good, batch_wrong],
            like_batches=[],
            active_trays_by_batch={
                "BGOOD": [1, 2, 3, 4, 5, 6, 7, 8, 9],
                "BWRONG": [1, 2, 3, 4, 5, 6, 7, 8, 9],
            },
            scdw_by_batch={"BGOOD": "VN501-502", "BWRONG": "503-504"},
        )
        payload = m.DmpPerfReportRequest(
            entries=[
                m.DmpPerfEntry(
                    batch_id="unused",
                    model="LR6",
                    groups=[
                        m.DmpPerfGroup(loai="UD", chuyen=c, trays=[])
                        for c in group_order
                    ],
                    raw_remark=raw_remark,
                )
            ]
        )
        m._compute_dmp_perf_groups(payload)
        return captured

    forward = _run("LR6 UD501 UD502 15", ["501", "502"])
    reversed_ = _run("LR6 UD502 UD501 15", ["502", "501"])

    # In both directions: BGOOD contributes the 4+4 split (501→1-4,
    # 502→5-8); BWRONG contributes nothing (scdw disjoint).
    fwd_good = sorted(v for (bid, _), v in forward.items() if bid == "BGOOD")
    rev_good = sorted(v for (bid, _), v in reversed_.items() if bid == "BGOOD")
    assert fwd_good == rev_good == [[1, 2, 3, 4], [5, 6, 7, 8]], (
        f"reversing the entry remark must not change scdw routing — "
        f"forward {fwd_good!r} vs reversed {rev_good!r}"
    )
    assert all(bid != "BWRONG" for (bid, _) in forward) and all(
        bid != "BWRONG" for (bid, _) in reversed_
    ), (
        f"disjoint-scdw batch must be excluded regardless of remark "
        f"order; forward={list(forward)} reversed={list(reversed_)}"
    )
