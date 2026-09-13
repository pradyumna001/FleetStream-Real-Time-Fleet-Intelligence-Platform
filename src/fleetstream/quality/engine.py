"""Compile ``rules.yml`` into Spark expressions and split records on them.

The engine does one thing that Great Expectations and Soda deliberately do not: a
row-level *split*. Those tools answer "did this dataset pass?" and are excellent at
it, but the platform needs each individual bad record diverted to quarantine with
its reason attached, while every good record continues to Silver in the same pass.
Expressing that as a dataset-level assertion would mean either failing the whole
micro-batch on one bad row, or scanning the data a second time to find the culprits.

Rules are loaded once and compiled into a single pass over the DataFrame that
produces an array of failed rule names per row, so adding a rule costs no extra
scan.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import Column, DataFrame

logger = logging.getLogger(__name__)

CRITICAL = "critical"
WARNING = "warning"
VALID_SEVERITIES = frozenset({CRITICAL, WARNING})

#: Column names the engine adds. Kept here so the streaming job can drop them
#: before writing, rather than hard-coding the same strings in two files.
FAILED_RULES_COL = "dq_failed_rules"
WARNINGS_COL = "dq_warnings"
IS_VALID_COL = "dq_is_valid"
ENGINE_COLUMNS = (FAILED_RULES_COL, WARNINGS_COL, IS_VALID_COL)


@dataclass(frozen=True)
class Rule:
    """One validation rule."""

    name: str
    severity: str
    expression: str
    description: str
    dimension: str = "validity"

    @property
    def is_critical(self) -> bool:
        return self.severity == CRITICAL


class RuleSetError(ValueError):
    """Raised when rules.yml is malformed. Loud on purpose - a silently dropped
    rule means bad data flowing to Silver unnoticed."""


@dataclass(frozen=True)
class RuleSet:
    version: int
    rules: tuple[Rule, ...]

    @property
    def critical(self) -> tuple[Rule, ...]:
        return tuple(r for r in self.rules if r.is_critical)

    @property
    def warnings(self) -> tuple[Rule, ...]:
        return tuple(r for r in self.rules if not r.is_critical)

    def by_name(self, name: str) -> Rule:
        for rule in self.rules:
            if rule.name == name:
                return rule
        raise KeyError(name)

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(r.name for r in self.rules)


def _parse(raw: dict[str, Any]) -> RuleSet:
    if not isinstance(raw, dict) or "rules" not in raw:
        raise RuleSetError("rules file must be a mapping containing a 'rules' key")

    version = int(raw.get("version", 1))
    parsed: list[Rule] = []
    seen: set[str] = set()

    for index, entry in enumerate(raw["rules"] or []):
        if not isinstance(entry, dict):
            raise RuleSetError(f"rule #{index} is not a mapping")
        missing = {"name", "severity", "expression"} - entry.keys()
        if missing:
            raise RuleSetError(f"rule #{index} is missing {sorted(missing)}")

        name = str(entry["name"]).strip()
        if name in seen:
            # Duplicate names would make quarantine counts ambiguous and make it
            # impossible to tell which of the two rules actually fired.
            raise RuleSetError(f"duplicate rule name: {name}")
        seen.add(name)

        severity = str(entry["severity"]).strip().lower()
        if severity not in VALID_SEVERITIES:
            raise RuleSetError(
                f"rule {name!r} has severity {severity!r}; "
                f"expected one of {sorted(VALID_SEVERITIES)}"
            )

        parsed.append(
            Rule(
                name=name,
                severity=severity,
                expression=str(entry["expression"]).strip(),
                description=" ".join(str(entry.get("description", "")).split()),
                dimension=str(entry.get("dimension", "validity")).strip().lower(),
            )
        )

    if not parsed:
        raise RuleSetError("no rules defined")
    return RuleSet(version=version, rules=tuple(parsed))


def load_rules(path: str | Path | None = None) -> RuleSet:
    """Load the rule set, defaulting to the packaged ``rules.yml``."""
    if path is None:
        text = resources.files("fleetstream.quality").joinpath("rules.yml").read_text("utf-8")
    else:
        text = Path(path).read_text(encoding="utf-8")
    return _parse(yaml.safe_load(text))


# ---------------------------------------------------------------------------
# Spark application
# ---------------------------------------------------------------------------


def _failed_rule_names(rules: tuple[Rule, ...]) -> Column:
    """Array of the names of every rule this row failed.

    ``NOT expr`` is deliberately wrapped so that a NULL result counts as a failure:
    in SQL a comparison against NULL is NULL, not false, and an unwrapped predicate
    would let a row with a null in a compared column slip through unchecked. Rules
    on optional fields permit null explicitly instead - see the note in rules.yml.
    """
    from pyspark.sql import functions as F

    flags = [
        F.when(~F.coalesce(F.expr(rule.expression), F.lit(False)), F.lit(rule.name))
        for rule in rules
    ]
    if not flags:
        return F.array().cast("array<string>")
    # array() keeps positional nulls for rules that passed; array_compact drops them.
    return F.array_compact(F.array(*flags))


def apply_rules(df: DataFrame, ruleset: RuleSet | None = None) -> DataFrame:
    """Annotate each row with the rules it failed.

    Adds three columns: the failed critical rules, the triggered warnings, and a
    boolean validity flag. The frame is otherwise untouched, so callers decide what
    to do with the verdict.
    """
    from pyspark.sql import functions as F

    rules = ruleset or load_rules()
    return (
        df.withColumn(FAILED_RULES_COL, _failed_rule_names(rules.critical))
        .withColumn(WARNINGS_COL, _failed_rule_names(rules.warnings))
        .withColumn(IS_VALID_COL, F.size(F.col(FAILED_RULES_COL)) == 0)
    )


def split(df: DataFrame, ruleset: RuleSet | None = None) -> tuple[DataFrame, DataFrame]:
    """Split into ``(valid, quarantined)``.

    The quarantined frame is exploded to one row per failed rule. A record breaking
    three rules produces three quarantine rows, which is what makes "how often does
    each rule fire" a simple GROUP BY instead of an array-unnesting exercise. The
    original record is carried on every row so any one of them can be replayed.
    """
    from pyspark.sql import functions as F

    rules = ruleset or load_rules()
    annotated = apply_rules(df, rules).cache()

    valid = annotated.filter(F.col(IS_VALID_COL)).drop(*ENGINE_COLUMNS)

    severity_map = F.create_map(
        *[x for rule in rules.rules for x in (F.lit(rule.name), F.lit(rule.severity))]
    )
    description_map = F.create_map(
        *[x for rule in rules.rules for x in (F.lit(rule.name), F.lit(rule.description))]
    )

    quarantined = (
        annotated.filter(~F.col(IS_VALID_COL))
        .withColumn("rule_name", F.explode(F.col(FAILED_RULES_COL)))
        .withColumn("rule_severity", severity_map[F.col("rule_name")])
        .withColumn("failure_reason", description_map[F.col("rule_name")])
        .drop(*ENGINE_COLUMNS)
    )
    return valid, quarantined


def rule_summary(ruleset: RuleSet | None = None) -> str:
    """Human-readable listing, used in job startup logs so a run records the rules
    it actually enforced rather than the ones someone believes are deployed."""
    rules = ruleset or load_rules()
    lines = [
        f"rule set v{rules.version}: {len(rules.rules)} rules "
        f"({len(rules.critical)} critical, {len(rules.warnings)} warning)"
    ]
    lines.extend(f"  [{r.severity:8s}] {r.name}" for r in rules.rules)
    return "\n".join(lines)
