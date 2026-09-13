"""Rule-set loading, validation and coverage of the injected corruptions."""

from __future__ import annotations

import textwrap

import pytest

from fleetstream.quality.engine import (
    CRITICAL,
    WARNING,
    RuleSetError,
    load_rules,
    rule_summary,
)
from fleetstream.quality.reconciliation import Counts
from fleetstream.simulator.generator import CORRUPTIONS

#: Which rule is expected to catch each corruption the simulator injects. This is the
#: contract between the simulator and the quality layer: if a rule is renamed or
#: removed without updating this map, the corruption would sail through unnoticed.
CORRUPTION_TO_RULE = {
    "null_vehicle_id": "vehicle_id_not_null",
    "negative_speed": "speed_non_negative",
    "fuel_out_of_range": "fuel_level_range",
    "latitude_out_of_range": "latitude_range",
    "longitude_out_of_range": "longitude_range",
    "battery_out_of_range": "battery_level_range",
    "missing_event_time": "event_time_not_null",
    "absurd_engine_temperature": "engine_temperature_range",
    "unparseable_speed": "speed_type_valid",
}


def write_rules(tmp_path, body: str):
    path = tmp_path / "rules.yml"
    path.write_text(textwrap.dedent(body), encoding="utf-8")
    return path


# -- the packaged rule set --------------------------------------------------


def test_packaged_rules_load():
    rules = load_rules()
    assert rules.version >= 1
    assert len(rules.rules) > 10


def test_every_injected_corruption_has_a_rule():
    names = set(load_rules().names)
    for corruption in CORRUPTIONS:
        rule = CORRUPTION_TO_RULE.get(corruption)
        assert rule is not None, f"{corruption} has no expected rule mapping"
        assert rule in names, f"{corruption} maps to missing rule {rule}"


def test_corruption_map_covers_every_corruption():
    assert set(CORRUPTION_TO_RULE) == set(CORRUPTIONS)


def test_rules_on_optional_fields_permit_null():
    """A SQL comparison against NULL is NULL, which the engine treats as a failure.
    Without an explicit null escape, an absent optional reading would be quarantined.
    """
    for rule in load_rules().rules:
        if any(
            token in rule.expression
            for token in ("speed >=", "fuel_level >=", "latitude >=", "battery_level >=")
        ):
            assert "IS NULL OR" in rule.expression, f"{rule.name} would reject nulls"


def test_schema_version_mismatch_is_a_warning_not_a_rejection():
    """Rejecting on version would turn a producer rollout into an outage."""
    rule = load_rules().by_name("schema_version_known")
    assert rule.severity == WARNING


def test_identity_and_time_rules_are_critical():
    rules = load_rules()
    for name in ("vehicle_id_not_null", "event_id_not_null", "event_time_not_null"):
        assert rules.by_name(name).severity == CRITICAL


def test_speed_ceiling_is_above_legitimate_overspeeding():
    """Real speeding must be analysed, not quarantined."""
    rule = load_rules().by_name("speed_within_plausible_max")
    assert "200" in rule.expression


def test_engine_temperature_range_admits_overheating():
    """The incident threshold is 100C; the validity ceiling must sit well above it."""
    rule = load_rules().by_name("engine_temperature_range")
    assert "200" in rule.expression


def test_summary_lists_every_rule():
    summary = rule_summary()
    for name in load_rules().names:
        assert name in summary


# -- malformed rule sets ----------------------------------------------------


def test_duplicate_rule_names_are_rejected(tmp_path):
    """Duplicates would make quarantine counts ambiguous."""
    path = write_rules(
        tmp_path,
        """
        version: 1
        rules:
          - {name: a, severity: critical, expression: "1 = 1"}
          - {name: a, severity: critical, expression: "2 = 2"}
        """,
    )
    with pytest.raises(RuleSetError, match="duplicate rule name"):
        load_rules(path)


def test_unknown_severity_is_rejected(tmp_path):
    path = write_rules(
        tmp_path,
        """
        version: 1
        rules:
          - {name: a, severity: catastrophic, expression: "1 = 1"}
        """,
    )
    with pytest.raises(RuleSetError, match="severity"):
        load_rules(path)


def test_missing_expression_is_rejected(tmp_path):
    path = write_rules(
        tmp_path,
        """
        version: 1
        rules:
          - {name: a, severity: critical}
        """,
    )
    with pytest.raises(RuleSetError, match="missing"):
        load_rules(path)


def test_empty_rule_set_is_rejected(tmp_path):
    """Silently enforcing nothing is the worst possible failure mode."""
    path = write_rules(tmp_path, "version: 1\nrules: []\n")
    with pytest.raises(RuleSetError, match="no rules"):
        load_rules(path)


def test_missing_rules_key_is_rejected(tmp_path):
    path = write_rules(tmp_path, "version: 1\n")
    with pytest.raises(RuleSetError, match="rules"):
        load_rules(path)


# -- reconciliation ---------------------------------------------------------


def test_counts_balance_when_every_row_is_accounted_for():
    counts = Counts(
        "r", "silver", source_count=100, duplicate_count=5, rejected_count=3, output_count=92
    )
    assert counts.balances
    assert counts.unaccounted == 0


def test_counts_detect_silent_row_loss():
    counts = Counts(
        "r", "silver", source_count=100, duplicate_count=5, rejected_count=3, output_count=80
    )
    assert not counts.balances
    assert counts.unaccounted == 12


def test_counts_detect_unexpected_row_growth():
    """More output than input means duplication, which is just as broken as loss."""
    counts = Counts("r", "silver", source_count=100, output_count=140)
    assert not counts.balances
    assert counts.unaccounted == -40
