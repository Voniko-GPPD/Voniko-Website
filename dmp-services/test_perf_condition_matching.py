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
