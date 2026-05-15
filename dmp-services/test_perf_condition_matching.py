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
    """For a single-group entry the helper returns the active-tray list as-is.

    The legacy ``_DMP_TRAY_ASSIGNMENT`` constant has been removed: there is
    no hardcoded "all 9 trays" fallback any more.  The single-line slot is
    just whatever active trays the caller supplies.
    """
    eff_groups = m._sort_eff_groups_for_tray_assignment(
        [{"loai": "UD+", "chuyen": "501", "trays": [], "_orig_idx": 0}]
    )
    assert len(eff_groups) == 1
    # Single line, all 9 physical trays measured: every tray is assigned to
    # the single group.
    assert m._split_active_trays_for_group_count(
        1, list(range(1, 10))
    ) == [list(range(1, 10))]


def test_dmp_tray_assignment_two_lines_tray1_damaged() -> None:
    """Spec example: tray 1 damaged → line 1 = [2,3,4,5], line 2 = [6,7,8,9]."""
    assert m._split_active_trays_for_group_count(
        2, [2, 3, 4, 5, 6, 7, 8, 9]
    ) == [[2, 3, 4, 5], [6, 7, 8, 9]]


def test_dmp_tray_assignment_two_lines_tray2_damaged() -> None:
    """Spec example: tray 2 damaged → line 1 = [1,3,4,5], line 2 = [6,7,8,9]."""
    assert m._split_active_trays_for_group_count(
        2, [1, 3, 4, 5, 6, 7, 8, 9]
    ) == [[1, 3, 4, 5], [6, 7, 8, 9]]


def test_dmp_tray_assignment_two_lines_tray5_damaged() -> None:
    """Spec example: tray 5 damaged → line 1 = [1,2,3,4], line 2 = [6,7,8,9]."""
    assert m._split_active_trays_for_group_count(
        2, [1, 2, 3, 4, 6, 7, 8, 9]
    ) == [[1, 2, 3, 4], [6, 7, 8, 9]]


def test_dmp_tray_assignment_no_hardcoded_fallback_when_empty() -> None:
    """No active trays → empty slots, never the legacy 1-4 / 5-8 fallback.

    The rewrite removed the ``_DMP_TRAY_ASSIGNMENT`` constant because the
    operator-facing requirement forbids any hardcoded tray index.  When the
    caller has no valid measurement data the helper returns empty groups so
    the report-rendering loop skips the entry instead of fabricating data on
    unmeasured trays.
    """
    assert m._split_active_trays_for_group_count(2, []) == [[], []]
    assert m._split_active_trays_for_group_count(1, []) == [[]]
    assert m._split_active_trays_for_group_count(3, []) == [[], [], []]
    assert not hasattr(m, "_DMP_TRAY_ASSIGNMENT"), (
        "Legacy _DMP_TRAY_ASSIGNMENT must be removed — it embedded the "
        "fixed 1-4/5-8 grouping the rewrite is meant to eliminate."
    )


# --------------------------------------------------------------------------- #
# DMP two-line tray allocation — fully-dynamic coverage.
#
# The operator-facing requirement is that the algorithm work for ANY tray
# failure combination, not just the few examples enumerated in the spec.
# These parametrized tests pin the dynamic behaviour by exhaustively asserting
# the same first-4 / next-4 / 8-tray-cap rule for every single-tray-damage
# case (9 cases), every two-tray-damage case (9C2 = 36 cases), and every
# possible subset of trays 1-9 (2^9 = 512 cases).  If a future change ever
# reintroduces a hardcoded tray range or special-cases an individual tray,
# at least one of these parametrized cases will fail.
# --------------------------------------------------------------------------- #


_ALL_TRAYS = list(range(1, 10))


def _expected_two_line_split(active: list[int]) -> list[list[int]]:
    """The single dynamic rule: scan trays in order, keep valid ones,
    first 4 → line 1, next 4 → line 2, drop anything past tray 8.
    """
    sorted_active = sorted(set(active))
    return [sorted_active[:4], sorted_active[4:8]]


@pytest.mark.parametrize("damaged", _ALL_TRAYS)
def test_dmp_tray_assignment_single_tray_damaged_fully_dynamic(damaged: int) -> None:
    """For EVERY single-tray-damage scenario the two-line split is the
    sequential first-4/next-4 of the remaining valid trays — no special
    handling for any individual tray index.
    """
    valid = [t for t in _ALL_TRAYS if t != damaged]
    expected = _expected_two_line_split(valid)
    assert m._split_active_trays_for_group_count(2, valid) == expected, (
        f"single-damage scenario for tray {damaged} must follow the "
        f"first-4/next-4 rule (no hardcoded mapping for tray {damaged})"
    )


_TWO_DAMAGE_CASES = [
    (d1, d2)
    for i, d1 in enumerate(_ALL_TRAYS)
    for d2 in _ALL_TRAYS[i + 1 :]
]


@pytest.mark.parametrize("d1,d2", _TWO_DAMAGE_CASES)
def test_dmp_tray_assignment_two_trays_damaged_fully_dynamic(d1: int, d2: int) -> None:
    """For EVERY two-tray-damage scenario the two-line split is the
    sequential first-4/next-4 of the remaining 7 valid trays.  Covers all
    36 combinations explicitly — including pairs the spec enumerated
    (1&2, 1&5, 2&7, 3&4, 3&9, 4&8, 5&6, 6&9, 7&8) and every other pair.
    Line 2 ends up with only 3 trays in every case, which downstream
    business rules may flag as incomplete.
    """
    valid = [t for t in _ALL_TRAYS if t not in (d1, d2)]
    expected = _expected_two_line_split(valid)
    got = m._split_active_trays_for_group_count(2, valid)
    assert got == expected, (
        f"two-damage scenario {{{d1},{d2}}} must follow the first-4/next-4 "
        f"rule (no hardcoded mapping for this pair)"
    )
    assert len(got[0]) == 4 and len(got[1]) == 3, (
        "with 7 valid trays, line 1 must have 4 and line 2 must have 3"
    )


def test_dmp_tray_assignment_exhaustive_subset_invariant() -> None:
    """Every one of the 512 possible subsets of trays 1-9 satisfies the
    same first-4/next-4 rule with the 8-tray cap.

    This is the strongest possible guarantee that the algorithm contains
    no hardcoded scenarios: if a special case existed for any specific
    tray combination it would break here.
    """
    from itertools import combinations

    checked = 0
    for size in range(len(_ALL_TRAYS) + 1):
        for combo in combinations(_ALL_TRAYS, size):
            valid = list(combo)
            got = m._split_active_trays_for_group_count(2, valid)
            assert got == _expected_two_line_split(valid), (
                f"subset {valid} broke the first-4/next-4 invariant: {got}"
            )
            assert len(got) == 2, f"must always return 2 slots, got {got}"
            assert len(got[0]) <= 4 and len(got[1]) <= 4, (
                f"per-line cap (4 trays) violated for subset {valid}: {got}"
            )
            checked += 1
    assert checked == 512, f"expected 2^9 = 512 subsets, checked {checked}"


def test_dmp_tray_assignment_seven_valid_trays_line_two_incomplete() -> None:
    """Spec example: ``[2,3,5,6,7,8,9]`` (7 valid) → line 1 = [2,3,5,6],
    line 2 = [7,8,9].  Line 2 only has 3 trays — downstream code may flag
    the dataset as incomplete, but the slot geometry is unambiguous.
    """
    assert m._split_active_trays_for_group_count(
        2, [2, 3, 5, 6, 7, 8, 9]
    ) == [[2, 3, 5, 6], [7, 8, 9]]


def test_dmp_tray_assignment_only_four_valid_trays_line_two_empty() -> None:
    """When only 4 valid trays exist (e.g. operator selected trays 5-8),
    line 1 receives all 4 and line 2 is empty.  No hardcoded fallback
    fills line 2 with a fictitious range.
    """
    assert m._split_active_trays_for_group_count(2, [5, 6, 7, 8]) == [
        [5, 6, 7, 8],
        [],
    ]
    assert m._split_active_trays_for_group_count(2, [1, 2, 3, 4]) == [
        [1, 2, 3, 4],
        [],
    ]
    assert m._split_active_trays_for_group_count(2, [6, 7, 8, 9]) == [
        [6, 7, 8, 9],
        [],
    ]


def test_dmp_tray_assignment_drops_only_extras_past_eight() -> None:
    """The 8-tray cap drops only trays past position 8 in the sorted
    valid-tray list — never specifically tray 9.  When tray 9 is the
    one damaged, all 8 remaining trays are used.
    """
    # All 9 valid → tray 9 dropped (it is the 9th in sorted order)
    assert m._split_active_trays_for_group_count(
        2, _ALL_TRAYS
    ) == [[1, 2, 3, 4], [5, 6, 7, 8]]
    # Tray 9 damaged → 8 remaining trays all used (none dropped)
    assert m._split_active_trays_for_group_count(
        2, [1, 2, 3, 4, 5, 6, 7, 8]
    ) == [[1, 2, 3, 4], [5, 6, 7, 8]]
    # Tray 1 damaged → trays 2-9 are all used (tray 9 NOT skipped)
    assert m._split_active_trays_for_group_count(
        2, [2, 3, 4, 5, 6, 7, 8, 9]
    ) == [[2, 3, 4, 5], [6, 7, 8, 9]]


def test_dmp_tray_assignment_explicit_trays_bypassed() -> None:
    """_sort_eff_groups_for_tray_assignment returns the list unchanged when
    explicit trays are already set (any(g['trays']) is True).
    """
    eff_groups = [{"loai": "UD", "chuyen": "501", "trays": [1, 2, 3, 4], "_orig_idx": 0}]
    result = m._sort_eff_groups_for_tray_assignment(eff_groups)
    assert result[0]["trays"] == [1, 2, 3, 4]
    assert any(g.get("trays") for g in result), "explicit trays must be detected"


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
    # Per-bz exact-match results: maps bz string → list[dict].
    # When set, a "WHERE bz = ?" call whose param matches a key returns the
    # associated list instead of the default *exact_batches*.  This lets tests
    # simulate the sibling-batch discovery path where the lookup bz differs
    # from the entry's raw_remark (e.g. "LR6 UD502 UD501 15" vs the entry's
    # "LR6 UD501 UD502 15").
    exact_batches_by_bz: dict[str, list[dict]] | None = None,
    # Per-id results: maps para_pub.id string → list[dict].
    # Used to simulate the direct "WHERE id = ?" batch-id lookup path.
    id_batches: dict[str, list[dict]] | None = None,
    # Per-batch scrq: maps batch_id → scrq date string for para_singl lookups.
    scrq_by_batch: dict[str, str] | None = None,
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

    *scdw_by_batch* supplies a per-batch ``para_singl.scdw`` value for tests
    that need to confirm scdw is **ignored** for routing (bz order is the sole
    source of truth for tray slot assignment).
    """
    if active_trays is None:
        active_trays = list(range(1, 10))
    if active_trays_by_batch is None:
        active_trays_by_batch = {}
    if scdw_by_batch is None:
        scdw_by_batch = {}
    if exact_batches_by_bz is None:
        exact_batches_by_bz = {}
    if id_batches is None:
        id_batches = {}
    if scrq_by_batch is None:
        scrq_by_batch = {}
    captured: dict[tuple[str, str], list[int]] = {}

    def fake_read_dmpdata(sql, params=()):
        if "FROM para_pub" in sql and "WHERE bz = ?" in sql:
            param_bz = str(params[0]) if params else ""
            if param_bz in exact_batches_by_bz:
                return list(exact_batches_by_bz[param_bz])
            return list(exact_batches or [])
        if "FROM para_pub" in sql and "WHERE bz LIKE ?" in sql:
            return list(like_batches or [])
        if "FROM para_pub" in sql and "WHERE id = ?" in sql:
            param_id = str(params[0]) if params else ""
            return list(id_batches.get(param_id, []))
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
            sid = str(params[0]) if params else ""
            scrq = scrq_by_batch.get(sid, "")
            return [{"scrq": scrq}] if scrq else []
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
# Bz-order positional split — various configurations
# --------------------------------------------------------------------------- #


def test_dmp_bz_positional_split_single_group_501(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A single-group entry for chuyen 501 against a 2-line bz="LR6 UD501
    UD502 15" must land on slot 0 (trays 1-4), not consume all 9 trays.
    Bz order is the canonical source — scdw values are ignored.
    """
    batch_a = _make_dmp_batch(batch_id="BA", bz="LR6 UD501 UD502 15")
    batch_b = _make_dmp_batch(batch_id="BB", bz="LR6 UD501 UD502 15")
    captured = _install_dmp_batch_fakes(
        monkeypatch,
        exact_batches=[batch_a, batch_b],
        like_batches=[],
        active_trays_by_batch={
            "BA": [1, 2, 3, 4],
            "BB": [6, 7, 8, 9],
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

    # bz="LR6 UD501 UD502 15" → slot 0=501, slot 1=502
    # Batch A (active=[1,2,3,4]): all 4 active → slot 0 → chuyen 501
    # Batch B (active=[6,7,8,9]): all 4 active → slot 0 → chuyen 501
    # (bz says 501 is first, so the first 4 active trays of every batch
    # go to slot 0 regardless of which physical trays are populated)
    a_trays = sorted(v for (bid, _), v in captured.items() if bid == "BA")
    assert a_trays == [[1, 2, 3, 4]], (
        f"batch A active trays (1-4) must map to slot 0 (chuyen 501); got {a_trays}"
    )


def test_dmp_bz_positional_split_two_groups(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two-group entry with bz="LR6 UD502 UD501 15" (reversed) and a full
    batch uses bz order: 502→slot 0, 501→slot 1.
    """
    batch_a = _make_dmp_batch(batch_id="BA", bz="LR6 UD502 UD501 15")
    captured = _install_dmp_batch_fakes(
        monkeypatch,
        exact_batches=[batch_a],
        like_batches=[],
        active_trays_by_batch={"BA": [1, 2, 3, 4, 6, 7, 8, 9]},
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
                raw_remark="LR6 UD502 UD501 15",
            )
        ]
    )

    m._compute_dmp_perf_groups(payload)

    all_calls = {k: v for k, v in captured.items() if k[0] == "BA"}
    # slot 0 = [1,2,3,4] → 502,  slot 1 = [6,7,8,9] → 501
    assert [1, 2, 3, 4] in all_calls.values(), (
        f"trays 1-4 (slot 0 / chuyen 502) must appear in captured; got {list(all_calls.values())}"
    )
    assert [6, 7, 8, 9] in all_calls.values(), (
        f"trays 6-9 (slot 1 / chuyen 501) must appear in captured; got {list(all_calls.values())}"
    )


def test_dmp_bz_positional_split_partial_entry_chuyen_501(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Single-group entry (chuyen 501) against bz="LR6 UD501 UD502":
    positional split places 501 in slot 0 (trays 1-4).
    """
    batch = _make_dmp_batch(batch_id="BFULL", bz="LR6 UD501 UD502")
    captured = _install_dmp_batch_fakes(
        monkeypatch,
        exact_batches=[],
        like_batches=[batch],
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
    assert trays == [[1, 2, 3, 4]], (
        f"bz positional split must place chuyen 501 in slot 0 (trays 1-4); got {trays}"
    )


def test_dmp_bz_positional_split_partial_entry_chuyen_502(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Single-group entry (chuyen 502) against bz="LR6 UD501 UD502":
    positional split places 502 in slot 1 (trays 5-8).
    """
    batch = _make_dmp_batch(batch_id="BNOSCDW", bz="LR6 UD501 UD502")
    captured = _install_dmp_batch_fakes(
        monkeypatch,
        exact_batches=[],
        like_batches=[batch],
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
    assert trays == [[5, 6, 7, 8]], (
        f"bz positional split must place chuyen 502 in slot 1 (trays 5-8); got {trays}"
    )


# --------------------------------------------------------------------------- #
# Single-batch reversed-bz tray assignment (Request #250 fix)
# --------------------------------------------------------------------------- #
# When a single DMP batch covers BOTH production lines and its bz has the
# lines in non-ascending order (e.g. "LR6 UD502 UD501 15"), the physical
# tray assignment MUST follow the bz order (502→slot 0, 501→slot 1) rather
# than the numerically sorted chuyen order (which would always put 501 first
# regardless of operator intent).  scdw is not used for routing.
#
# Root cause: (1) canonical was picked via strict ``>`` so equal-length bz and
# entry remark fell back to entry order; (2) legacy fallthrough only built
# chuyen_to_pos when ``canon_n > n_eff``, leaving it empty and falling back
# to sorted-chuyen-based g_idx when both counts were equal.
# --------------------------------------------------------------------------- #


def test_dmp_single_full_batch_reversed_bz_maps_to_correct_trays(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A single batch covering both lines with bz='LR6 UD502 UD501 15'
    (reversed) and scdw='VN501-502' must assign trays 1-4 to production
    line 502 (first in bz) and trays 6-8 to production line 501 (second
    in bz).  The old code sorted by chuyen number and always put 501 on
    the lower-index slot, giving the wrong physical-slot mapping.

    Use 7 active trays (4 in slot 0, 3 in slot 1) so the avg_count in
    the returned groups dict can distinguish which sheet got which slot.
    """
    batch = _make_dmp_batch(batch_id="BREV", bz="LR6 UD502 UD501 15")
    _install_dmp_batch_fakes(
        monkeypatch,
        exact_batches=[batch],
        like_batches=[],
        # 7 active trays → split-for-2 → [[1,2,3,4], [6,7,8]]
        # slot 0 has 4 trays, slot 1 has 3 trays — distinguishable by avg_count.
        active_trays_by_batch={"BREV": [1, 2, 3, 4, 6, 7, 8]},
        scdw_by_batch={"BREV": "VN501-502"},
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
                raw_remark="LR6 UD502 UD501 15",
            )
        ]
    )

    groups = m._compute_dmp_perf_groups(payload)

    def _avg_count(sheet: str) -> int:
        """Return avg_count from the first perf value in a sheet."""
        for row_data in groups.get(sheet, {}).values():
            for perf in row_data.values():
                if isinstance(perf, dict) and "avg_count" in perf:
                    return perf["avg_count"]
        return -1

    # bz says 502 first → 502 owns slot 0 (4 trays → avg_count=4)
    # bz says 501 second → 501 owns slot 1 (3 trays → avg_count=3)
    assert "LR6 502" in groups, "LR6 502 sheet must be present"
    assert "LR6 501" in groups, "LR6 501 sheet must be present"
    c502 = _avg_count("LR6 502")
    c501 = _avg_count("LR6 501")
    assert c502 == 4, (
        f"reversed bz: 502 must occupy slot 0 (4 active trays → avg_count=4); "
        f"got avg_count={c502}"
    )
    assert c501 == 3, (
        f"reversed bz: 501 must occupy slot 1 (3 active trays → avg_count=3); "
        f"got avg_count={c501}"
    )


def test_dmp_single_full_batch_ascending_bz_unaffected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sanity-check the well-behaved case: bz='LR6 UD501 UD502' (ascending)
    with scdw='VN501-502' must still assign 501→slot 0 (4 trays) and
    502→slot 1 (3 trays).  The fix must not regress the common case.
    """
    batch = _make_dmp_batch(batch_id="BASC", bz="LR6 UD501 UD502")
    _install_dmp_batch_fakes(
        monkeypatch,
        exact_batches=[],
        like_batches=[batch],
        active_trays_by_batch={"BASC": [1, 2, 3, 4, 6, 7, 8]},
        scdw_by_batch={"BASC": "VN501-502"},
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

    groups = m._compute_dmp_perf_groups(payload)

    def _avg_count(sheet: str) -> int:
        for row_data in groups.get(sheet, {}).values():
            for perf in row_data.values():
                if isinstance(perf, dict) and "avg_count" in perf:
                    return perf["avg_count"]
        return -1

    assert "LR6 501" in groups, "LR6 501 sheet must be present"
    assert "LR6 502" in groups, "LR6 502 sheet must be present"
    c501 = _avg_count("LR6 501")
    c502 = _avg_count("LR6 502")
    assert c501 == 4, (
        f"ascending bz: 501 must keep slot 0 (4 trays → avg_count=4); "
        f"got avg_count={c501}"
    )
    assert c502 == 3, (
        f"ascending bz: 502 must keep slot 1 (3 trays → avg_count=3); "
        f"got avg_count={c502}"
    )


# --------------------------------------------------------------------------- #
# Entry-order g_idx fallback (Request #251 audit)
# --------------------------------------------------------------------------- #
# When the bz cannot be parsed (or canonical is empty), _resolve_dmp_tray_list
# falls back to auto_trays[g_idx].  The DMP path must NOT sort eff_groups by
# chuyen number before this fallback runs, because "lower chuyen → lower tray
# slot" is the wrong assumption.  The entry's group order (which mirrors the bz
# remark order since the frontend parses left-to-right) is the correct fallback.
# --------------------------------------------------------------------------- #


def test_dmp_entry_order_used_as_gidx_fallback_when_bz_unparseable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the batch bz contains no parseable UD/HP group tokens and the
    entry remark also parses to nothing, ``chuyen_to_pos`` is empty and
    _resolve_dmp_tray_list falls back to ``auto_trays[g_idx]``.  The DMP
    path must preserve the entry's group order (bz order) so g_idx=0 maps
    to the first group in the entry — NOT to the lowest-numbered chuyen.

    Here the entry lists 502 before 501 (matching the physical bz order).
    With 7 active trays (4+3 split), group 0 (chuyen 502) must get slot 0
    (4 trays → avg_count=4) and group 1 (chuyen 501) must get slot 1
    (3 trays → avg_count=3).  If the DMP path were still sorting by chuyen
    number, 501 would end up in slot 0 and get 4 trays — the wrong result.
    """
    # bz is a number-only string that _parse_bz_groups cannot parse into groups
    # (no UD/UDP/HP prefix), forcing chuyen_to_pos to be empty.
    batch = _make_dmp_batch(batch_id="BUNPARSEABLE", bz="LR6 502 501")
    _install_dmp_batch_fakes(
        monkeypatch,
        exact_batches=[batch],
        like_batches=[],
        active_trays_by_batch={"BUNPARSEABLE": [1, 2, 3, 4, 6, 7, 8]},
    )

    payload = m.DmpPerfReportRequest(
        entries=[
            m.DmpPerfEntry(
                batch_id="unused",
                model="LR6",
                groups=[
                    # Entry order: 502 first, then 501 — mirrors bz physical order
                    m.DmpPerfGroup(loai="UD", chuyen="502", trays=[]),
                    m.DmpPerfGroup(loai="UD", chuyen="501", trays=[]),
                ],
                raw_remark="LR6 502 501",
            )
        ]
    )

    groups = m._compute_dmp_perf_groups(payload)

    def _avg_count(sheet: str) -> int:
        for row_data in groups.get(sheet, {}).values():
            for perf in row_data.values():
                if isinstance(perf, dict) and "avg_count" in perf:
                    return perf["avg_count"]
        return -1

    # Entry group 0 = chuyen 502 → auto_trays[0] = [1,2,3,4] (4 trays)
    # Entry group 1 = chuyen 501 → auto_trays[1] = [6,7,8]   (3 trays)
    assert "LR6 502" in groups, "LR6 502 sheet must be present"
    assert "LR6 501" in groups, "LR6 501 sheet must be present"
    c502 = _avg_count("LR6 502")
    c501 = _avg_count("LR6 501")
    assert c502 == 4, (
        f"entry-order fallback: 502 (g_idx=0) must get slot 0 (4 trays); "
        f"got avg_count={c502}"
    )
    assert c501 == 3, (
        f"entry-order fallback: 501 (g_idx=1) must get slot 1 (3 trays); "
        f"got avg_count={c501}"
    )


# --------------------------------------------------------------------------- #
# Per-batch positional routing (Request #251 follow-up)
# --------------------------------------------------------------------------- #
# When the DMP machine stores one para_pub record PER production line (each
# sharing the same composite bz such as "LR6 UD501 UD502 15"), each batch
# returned by the exact-match query covers only one line's batteries.  The
# system must assign each batch to the corresponding canonical line by
# position: batch_rows[0] → first line in bz, batch_rows[1] → second line.
# This is determined purely from bz order — scdw is never consulted.
# --------------------------------------------------------------------------- #


def test_dmp_two_per_line_batches_ud501_ud502_15_both_load(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression test for the specific failing case reported in the problem
    statement: remark 'LR6 UD501 UD502 15' with two separate per-line
    para_pub records (each carrying only one line's batteries).

    Expected behaviour:
      - batch_rows[0] (trays 1-4) → assigned to line 501 (first in bz).
      - batch_rows[1] (trays 5-8) → assigned to line 502 (second in bz).

    Before the fix both batches were split positionally with
    _split_active_trays_for_group_count(2, [4 trays]) which always gave
    slot 0 = [4 trays] and slot 1 = [], so line 502 never received any data.
    """
    batch_a = _make_dmp_batch(batch_id="BA", bz="LR6 UD501 UD502 15")
    batch_b = _make_dmp_batch(batch_id="BB", bz="LR6 UD501 UD502 15")
    captured = _install_dmp_batch_fakes(
        monkeypatch,
        exact_batches=[batch_a, batch_b],
        like_batches=[],
        active_trays_by_batch={
            "BA": [1, 2, 3, 4],
            "BB": [5, 6, 7, 8],
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

    groups = m._compute_dmp_perf_groups(payload)

    # batch_rows[0] (BA) → line 501: all 4 trays go to 501.
    a_trays = [v for (bid, _), v in captured.items() if bid == "BA"]
    assert a_trays == [[1, 2, 3, 4]], (
        f"per-batch mode: batch_rows[0] must route to line 501 (trays 1-4); got {a_trays}"
    )
    # batch_rows[1] (BB) → line 502: all 4 trays go to 502.
    b_trays = [v for (bid, _), v in captured.items() if bid == "BB"]
    assert b_trays == [[5, 6, 7, 8]], (
        f"per-batch mode: batch_rows[1] must route to line 502 (trays 5-8); got {b_trays}"
    )
    # Both sheets must contain data.
    assert "LR6 501" in groups, "sheet 'LR6 501' must be present"
    assert "LR6 502" in groups, "sheet 'LR6 502' must be present"


def test_dmp_two_per_line_batches_reversed_bz_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same per-batch scenario with reversed bz ('LR6 UD502 UD501 15').

    The remark defines: line 502 first, line 501 second.  Regardless of scdw
    or any other field, the assignment must follow bz order:
      - batch_rows[0] (trays 1-4) → line 502 (first in bz).
      - batch_rows[1] (trays 5-8) → line 501 (second in bz).
    """
    batch_a = _make_dmp_batch(batch_id="BA_REV", bz="LR6 UD502 UD501 15")
    batch_b = _make_dmp_batch(batch_id="BB_REV", bz="LR6 UD502 UD501 15")
    captured = _install_dmp_batch_fakes(
        monkeypatch,
        exact_batches=[batch_a, batch_b],
        like_batches=[],
        active_trays_by_batch={
            "BA_REV": [1, 2, 3, 4],
            "BB_REV": [5, 6, 7, 8],
        },
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
                raw_remark="LR6 UD502 UD501 15",
            )
        ]
    )

    groups = m._compute_dmp_perf_groups(payload)

    # batch_rows[0] (BA_REV, trays 1-4) → line 502 (first in bz).
    a_trays = [v for (bid, _), v in captured.items() if bid == "BA_REV"]
    assert a_trays == [[1, 2, 3, 4]], (
        f"reversed bz: batch_rows[0] must go to line 502 (trays 1-4); got {a_trays}"
    )
    # batch_rows[1] (BB_REV, trays 5-8) → line 501 (second in bz).
    b_trays = [v for (bid, _), v in captured.items() if bid == "BB_REV"]
    assert b_trays == [[5, 6, 7, 8]], (
        f"reversed bz: batch_rows[1] must go to line 501 (trays 5-8); got {b_trays}"
    )
    assert "LR6 502" in groups, "sheet 'LR6 502' must be present"
    assert "LR6 501" in groups, "sheet 'LR6 501' must be present"


def test_dmp_single_combined_batch_unaffected_by_per_batch_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When there is only ONE batch for a two-line bz (not one per line),
    per-batch mode must NOT activate and the normal positional tray split
    must apply so that both production lines receive their correct trays.
    """
    # Single batch with all 8 batteries.
    batch = _make_dmp_batch(batch_id="BSINGLE", bz="LR6 UD501 UD502 15")
    captured = _install_dmp_batch_fakes(
        monkeypatch,
        exact_batches=[batch],
        like_batches=[],
        active_trays_by_batch={"BSINGLE": [1, 2, 3, 4, 5, 6, 7, 8]},
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

    # Normal 2-line split: [[1,2,3,4], [5,6,7,8]]
    # 501 → slot 0 → [1,2,3,4],  502 → slot 1 → [5,6,7,8]
    all_trays = [v for (bid, _), v in captured.items() if bid == "BSINGLE"]
    assert [1, 2, 3, 4] in all_trays, (
        f"single combined batch: chuyen 501 must take trays [1-4]; got {all_trays}"
    )
    assert [5, 6, 7, 8] in all_trays, (
        f"single combined batch: chuyen 502 must take trays [5-8]; got {all_trays}"
    )


def test_dmp_per_batch_mode_extra_batches_beyond_canonical_use_split(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When there are more batches than canonical lines, the first N batches
    use per-batch mode (batch[i] → canonical_line[i]) and the remaining
    batches fall back to the normal positional tray split.

    Setup: 2-line bz, 3 batches.  batch[2] has all 8 batteries and should
    produce data for both lines via the regular split.
    """
    batch_a = _make_dmp_batch(batch_id="B0", bz="LR6 UD501 UD502 15", fdrq="2026-05-01")
    batch_b = _make_dmp_batch(batch_id="B1", bz="LR6 UD501 UD502 15", fdrq="2026-05-01")
    batch_c = _make_dmp_batch(batch_id="B2", bz="LR6 UD501 UD502 15", fdrq="2026-04-01")
    captured = _install_dmp_batch_fakes(
        monkeypatch,
        exact_batches=[batch_a, batch_b, batch_c],
        like_batches=[],
        active_trays_by_batch={
            "B0": [1, 2, 3, 4],
            "B1": [5, 6, 7, 8],
            "B2": [1, 2, 3, 4, 5, 6, 7, 8],
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

    # B0 (batch_line_idx=0): all trays → line 501 only.
    b0_trays = [v for (bid, _), v in captured.items() if bid == "B0"]
    assert b0_trays == [[1, 2, 3, 4]], f"B0 must route to 501 only; got {b0_trays}"

    # B1 (batch_line_idx=1): all trays → line 502 only.
    b1_trays = [v for (bid, _), v in captured.items() if bid == "B1"]
    assert b1_trays == [[5, 6, 7, 8]], f"B1 must route to 502 only; got {b1_trays}"

    # B2 (index 2 >= _n_canonical=2): normal positional split → both lines.
    b2_trays = sorted([v for (bid, _), v in captured.items() if bid == "B2"])
    assert [1, 2, 3, 4] in b2_trays, f"B2 (normal split): 501 must take [1-4]; got {b2_trays}"
    assert [5, 6, 7, 8] in b2_trays, f"B2 (normal split): 502 must take [5-8]; got {b2_trays}"


def test_per_batch_mode_assigns_by_tray_slot_not_db_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression: database returns the batch for line 502 FIRST (e.g. because
    Access MDB insertion order differs from bz token order) even though bz
    says 'LR6 UD501 UD502 15'.

    The tray-slot-based assignment must ignore database row order and assign
    each batch to the canonical slot whose sequential tray range its active
    trays fall into:
      - BB (trays 5-8) → slot 1 (502, second in bz) regardless of being first in batch_rows.
      - BA (trays 1-4) → slot 0 (501, first in bz) regardless of being second in batch_rows.

    Before the fix batch_rows[0]=BB would be hard-coded to slot 0 (501) causing
    502 to receive no data and 501 to receive wrong batteries.
    """
    # Database returns BB (502's batteries, trays 5-8) FIRST.
    batch_b = _make_dmp_batch(batch_id="BB", bz="LR6 UD501 UD502 15")
    batch_a = _make_dmp_batch(batch_id="BA", bz="LR6 UD501 UD502 15")
    captured = _install_dmp_batch_fakes(
        monkeypatch,
        exact_batches=[batch_b, batch_a],  # BB first — reversed DB order
        like_batches=[],
        active_trays_by_batch={
            "BA": [1, 2, 3, 4],  # line 501's physical batteries
            "BB": [5, 6, 7, 8],  # line 502's physical batteries
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

    groups = m._compute_dmp_perf_groups(payload)

    # BA (trays 1-4) must always route to line 501 (slot 0 in bz order).
    a_trays = [v for (bid, _), v in captured.items() if bid == "BA"]
    assert a_trays == [[1, 2, 3, 4]], (
        f"DB-reverse: BA (line 501 batteries) must take trays [1-4]; got {a_trays}"
    )
    # BB (trays 5-8) must always route to line 502 (slot 1 in bz order).
    b_trays = [v for (bid, _), v in captured.items() if bid == "BB"]
    assert b_trays == [[5, 6, 7, 8]], (
        f"DB-reverse: BB (line 502 batteries) must take trays [5-8]; got {b_trays}"
    )
    assert "LR6 501" in groups, "sheet 'LR6 501' must be present"
    assert "LR6 502" in groups, "sheet 'LR6 502' must be present"
    # Both sheets must contain performance data (non-empty row dict).
    assert groups["LR6 501"], "sheet 'LR6 501' must contain performance rows"
    assert groups["LR6 502"], "sheet 'LR6 502' must contain performance rows"


# --------------------------------------------------------------------------- #
# Sibling-batch discovery and _all_same_bz normalisation (Request #263 fixes)
# --------------------------------------------------------------------------- #


def test_dmp_sibling_lookup_activates_per_batch_when_batch_id_only_finds_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Root-cause regression: operator typed 'LR6 UD502 UD501 15' (reversed)
    in the DMP machine for both per-line batches, but the web UI entry stores
    raw_remark='LR6 UD501 UD502 15' (correct order).

    Exact-match lookup with the entry's raw_remark finds nothing (wrong word
    order in DB), LIKE lookup also fails (substring not found), and the direct
    batch_id lookup finds only ONE of the two batches.

    The sibling-batch discovery path must then query para_pub again using the
    ONE found batch's own bz field ('LR6 UD502 UD501 15') and return BOTH
    per-line batches, enabling per-batch mode and loading both lines.
    """
    # DB has two per-line batches with reversed bz.
    batch_a = _make_dmp_batch(
        batch_id="BA_SIBLING",
        bz="LR6 UD502 UD501 15",   # reversed (as operator typed in DMP machine)
        fdrq="2026-04-20",
    )
    batch_b = _make_dmp_batch(
        batch_id="BB_SIBLING",
        bz="LR6 UD502 UD501 15",   # same reversed bz
        fdrq="2026-04-02",
    )

    captured = _install_dmp_batch_fakes(
        monkeypatch,
        # Exact match with raw_remark='LR6 UD501 UD502 15' → nothing (wrong order).
        exact_batches=[],
        # LIKE '%LR6 UD501 UD502%' → nothing (different word order).
        like_batches=[],
        # Direct batch_id lookup for 'BA_SIBLING' → finds batch_a only.
        id_batches={"BA_SIBLING": [batch_a]},
        # Sibling lookup with batch_a.bz='LR6 UD502 UD501 15' → both batches.
        exact_batches_by_bz={"LR6 UD502 UD501 15": [batch_a, batch_b]},
        active_trays_by_batch={
            "BA_SIBLING": [1, 2, 3, 4],   # UD502's physical batteries (bz slot 0)
            "BB_SIBLING": [5, 6, 7, 8],   # UD501's physical batteries (bz slot 1)
        },
    )

    payload = m.DmpPerfReportRequest(
        entries=[
            m.DmpPerfEntry(
                batch_id="BA_SIBLING",   # points to only one batch
                model="LR6",
                groups=[
                    m.DmpPerfGroup(loai="UD", chuyen="501", trays=[]),
                    m.DmpPerfGroup(loai="UD", chuyen="502", trays=[]),
                ],
                raw_remark="LR6 UD501 UD502 15",
            )
        ]
    )

    groups = m._compute_dmp_perf_groups(payload)

    # Both sheets must be present and contain data.
    # (Datasets are swapped relative to entry order because bz has reversed
    # chuyen order — that is expected and documented behaviour.)
    assert "LR6 501" in groups, "sibling lookup: sheet 'LR6 501' must be present"
    assert "LR6 502" in groups, "sibling lookup: sheet 'LR6 502' must be present"
    assert groups["LR6 501"], "sibling lookup: 'LR6 501' must contain performance rows"
    assert groups["LR6 502"], "sibling lookup: 'LR6 502' must contain performance rows"

    # In per-batch mode each batch must be routed to exactly one line.
    # With reversed bz ('LR6 UD502 UD501 15'), slot 0 = UD502, slot 1 = UD501.
    # BA_SIBLING (trays 1-4) → _pb_canonical_slot → slot 0 (UD502) → "LR6 502".
    # BB_SIBLING (trays 5-8) → _pb_canonical_slot → slot 1 (UD501) → "LR6 501".
    all_ba = [v for (bid, _), v in captured.items() if bid == "BA_SIBLING"]
    all_bb = [v for (bid, _), v in captured.items() if bid == "BB_SIBLING"]
    assert all_ba, "BA_SIBLING must be computed (not silently dropped)"
    assert all_bb, "BB_SIBLING must be computed (not silently dropped)"


def test_dmp_all_same_bz_normalised_ignores_15_suffix_difference(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_all_same_bz must be True when LIKE returns two batches with the same
    canonical production-line chuyens but different Q/15 suffixes
    (e.g. 'LR6 UD501 UD502 15' and 'LR6 UD501 UD502' — same lines, different
    test-flag).  Before the fix the exact-string check made _all_same_bz=False,
    disabled per-batch mode, and caused UD502 to disappear.
    """
    # LIKE returns two batches: one with '15' suffix, one without.
    batch_a = _make_dmp_batch(
        batch_id="B15", bz="LR6 UD501 UD502 15", fdrq="2026-04-18"
    )
    batch_b = _make_dmp_batch(
        batch_id="BNORMAL", bz="LR6 UD501 UD502", fdrq="2026-03-10"
    )

    captured = _install_dmp_batch_fakes(
        monkeypatch,
        exact_batches=[],   # no exact match (e.g. batch_id-only path used first)
        like_batches=[batch_a, batch_b],
        active_trays_by_batch={
            "B15":     [1, 2, 3, 4],
            "BNORMAL": [5, 6, 7, 8],
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

    groups = m._compute_dmp_perf_groups(payload)

    # Per-batch mode must activate (same chuyen set regardless of '15' suffix).
    assert "LR6 501" in groups, "normalised _all_same_bz: 'LR6 501' must be present"
    assert "LR6 502" in groups, "normalised _all_same_bz: 'LR6 502' must be present"
    assert groups["LR6 501"], "'LR6 501' must have data"
    assert groups["LR6 502"], "'LR6 502' must have data"

    # With per-batch mode: B15 (trays 1-4) → slot 0 (UD501), BNORMAL (5-8) → slot 1 (UD502).
    b15_trays = [v for (bid, _), v in captured.items() if bid == "B15"]
    bnormal_trays = [v for (bid, _), v in captured.items() if bid == "BNORMAL"]
    assert b15_trays == [[1, 2, 3, 4]], (
        f"B15 must route to UD501 (slot 0, trays 1-4); got {b15_trays}"
    )
    assert bnormal_trays == [[5, 6, 7, 8]], (
        f"BNORMAL must route to UD502 (slot 1, trays 5-8); got {bnormal_trays}"
    )


def test_dmp_null_id_extra_batch_uses_index_key_not_main_batch_trays(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When an extra batch has a null/falsy id (para_pub.id returns None after
    _dm2000_get_value filtering), its _pb_all_active key must be an index-based
    synthetic key rather than falling back to entry.batch_id.

    Without the fix, the extra batch's _xb_id falls back to entry.batch_id
    which equals actual_batch_id, causing the extra batch to pick up the MAIN
    batch's active trays from _pb_all_active.  Both batches then route to the
    same slot (UD501) and UD502 never receives any data.

    With the fix, the null-id extra batch gets its own synthetic key and its
    tray lookup returns [] (no para_singl rows for empty id), so it produces
    no data for any line — but crucially it does NOT contaminate UD501 with a
    duplicate write or block per-batch-mode from assigning trays correctly.
    """
    # batch_a: valid id "MAIN_ID"
    batch_a = _make_dmp_batch(batch_id="MAIN_ID", bz="LR6 UD501 UD502 15")
    # batch_b: None id — _dm2000_get_value will filter this out as a null-like
    # value and return None; simulated here by using "None" as the id string
    # (which _dm2000_get_value treats as null).
    batch_b = dict(_make_dmp_batch(batch_id="UNUSED", bz="LR6 UD501 UD502 15"))
    batch_b["id"] = "None"   # _dm2000_get_value treats "None" as null-like

    captured = _install_dmp_batch_fakes(
        monkeypatch,
        exact_batches=[batch_a, batch_b],
        like_batches=[],
        active_trays_by_batch={
            "MAIN_ID": [1, 2, 3, 4],
            # "" (empty id) → falls back to default active_trays or []
        },
        active_trays=[],   # default to no trays so null-id batch gets nothing
    )

    payload = m.DmpPerfReportRequest(
        entries=[
            m.DmpPerfEntry(
                batch_id="MAIN_ID",
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

    # MAIN_ID must route to UD501 (slot 0, trays 1-4) exactly once.
    main_trays = [v for (bid, _), v in captured.items() if bid == "MAIN_ID"]
    assert main_trays == [[1, 2, 3, 4]], (
        f"null-id guard: MAIN_ID must route to UD501 (trays 1-4) exactly once; "
        f"got {main_trays}"
    )


# --------------------------------------------------------------------------- #
# DM3000 module — parameterised parallel of DM2000 with mA discharge unit
# --------------------------------------------------------------------------- #


def test_dm3000_module_config_distinct_from_dm2000():
    """DM2000_MOD and DM3000_MOD must be distinct DmModule instances with
    independent caches, paths and units.  This is the contract that lets the
    parameterised endpoint handlers serve both modules without state leaks."""
    assert m.DM2000_MOD is not m.DM3000_MOD
    assert m.DM2000_MOD.name == "dm2000"
    assert m.DM3000_MOD.name == "dm3000"
    assert m.DM2000_MOD.unit_suffix == "ohm"
    assert m.DM3000_MOD.unit_suffix == "mA"
    # Independent in-memory caches — mutating one must not affect the other.
    assert m.DM2000_MOD.archives_cache is not m.DM3000_MOD.archives_cache
    assert m.DM2000_MOD.curve_cache is not m.DM3000_MOD.curve_cache
    assert m.DM2000_MOD.archives_cache_lock is not m.DM3000_MOD.archives_cache_lock


def test_dm3000_module_default_paths():
    """Default DM3000 paths must match the supplier-app layout the user
    described in the task: ``D:\\DM3000\\dmdatabase`` for the source MDB
    files, and ``./dm3000_templates`` / ``./dm3000_perf_templates`` for the
    workbook templates.  The module-level constants drive cfg.get_ls_path /
    cfg.get_main_path so the paths must round-trip through DM3000_MOD."""
    import os
    # Only assert the default layout when the env var is unset, so this test
    # remains valid in dev environments that override DM3000_DATA_DIR.
    if not os.environ.get("DM3000_DATA_DIR"):
        assert m.DM3000_DATA_DIR.endswith("dmdatabase")
        assert "DM3000" in m.DM3000_DATA_DIR
        assert m.DM3000_MOD.get_ls_path().endswith("dmdata_ls.mdb")
        assert m.DM3000_MOD.get_main_path().endswith("DM3000.mdb")
    if not os.environ.get("DM3000_TEMPLATES_DIR"):
        assert m.DM3000_TEMPLATES_DIR.endswith("dm3000_templates")
    if not os.environ.get("DM3000_PERF_TEMPLATES_DIR"):
        assert m.DM3000_PERF_TEMPLATES_DIR.endswith("dm3000_perf_templates")


def test_dm3000_dis_condition_uses_mA_unit():
    """A bare numeric load_resistance (e.g. ``35``) must be auto-suffixed with
    ``mA`` for DM3000 — exactly matching the ``35mA,24h/d to 0.90V`` /
    ``1000mA,24h/d to 5.40V`` strings shown in the supplier-app screenshots
    captured for this task."""
    archive = {
        "load_resistance": "35",
        "fdfs": "24h/d",
        "endpoint_voltage": "0.90",
    }
    label = m._build_dis_condition_display(m.DM3000_MOD, archive)
    assert label == "35mA 24h/d to 0.90V", label

    archive2 = {
        "load_resistance": "1000",
        "fdfs": "24h/d",
        "endpoint_voltage": "5.40",
    }
    label2 = m._build_dis_condition_display(m.DM3000_MOD, archive2)
    assert label2 == "1000mA 24h/d to 5.40V", label2


def test_dm2000_dis_condition_still_uses_ohm_unit():
    """Regression: parameterising _build_dis_condition_display must not change
    DM2000's existing ohm output — the function must still produce the same
    ``620ohm 4m/h,8h/d to 0.900V`` string after the cfg refactor."""
    archive = {
        "load_resistance": "620",
        "fdfs": "4m/h,8h/d",
        "endpoint_voltage": "0.900",
    }
    label = m._build_dis_condition_display(m.DM2000_MOD, archive)
    assert label == "620ohm 4m/h,8h/d to 0.900V", label


def test_dm3000_preserves_explicit_unit_in_load_resistance():
    """When the raw value already contains text (e.g. ``1000mA`` or
    ``620+10k``) it must be passed through unchanged — no auto-suffix.  This
    protects DM3000 from double-suffixing values the operator entered with
    an explicit unit."""
    archive = {
        "load_resistance": "1000mA",
        "fdfs": "24h/d",
        "endpoint_voltage": "5.40",
    }
    assert m._build_dis_condition_display(m.DM3000_MOD, archive) == "1000mA 24h/d to 5.40V"


def test_dm_modules_registry_maps_prefixes():
    """_resolve_dm_module is driven by request.url.path containing /dm2000/
    or /dm3000/.  The registry that backs it must expose both prefixes."""
    assert "dm2000" in m.DM_MODULES
    assert "dm3000" in m.DM_MODULES
    assert m.DM_MODULES["dm2000"] is m.DM2000_MOD
    assert m.DM_MODULES["dm3000"] is m.DM3000_MOD


def test_dm3000_routes_registered_in_fastapi_app():
    """Every /dm2000/* endpoint must have a parallel /dm3000/* registration
    so the proxy can route DM3000 requests without code changes per call."""
    from fastapi.routing import APIRoute
    paths = {r.path for r in m.app.routes if isinstance(r, APIRoute)}
    dm2000_paths = {p for p in paths if p.startswith("/dm2000/")}
    assert dm2000_paths, "no /dm2000/* routes registered — refactor regression"
    for p in dm2000_paths:
        mirror = p.replace("/dm2000/", "/dm3000/", 1)
        assert mirror in paths, f"missing DM3000 mirror for {p}"
