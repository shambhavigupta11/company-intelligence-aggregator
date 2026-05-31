"""Data quality checks: row counts, freshness, schema drift, null rates."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd


@dataclass
class DQResult:
    check_name: str
    table: str
    passed: bool
    severity: str
    message: str
    measured_at: datetime


def row_count_anomaly(
    df: pd.DataFrame, table: str, baseline: list[int], sigma: float = 3.0
) -> DQResult:
    """Flag when current row count deviates >sigma standard deviations from baseline."""
    current = len(df)
    if not baseline:
        return DQResult(
            check_name="row_count_anomaly",
            table=table,
            passed=True,
            severity="info",
            message=f"no baseline yet; current={current}",
            measured_at=datetime.now(timezone.utc),
        )

    mean = float(np.mean(baseline))
    std = float(np.std(baseline)) or 1.0
    z = (current - mean) / std
    passed = abs(z) <= sigma
    return DQResult(
        check_name="row_count_anomaly",
        table=table,
        passed=passed,
        severity="warning" if not passed else "info",
        message=f"current={current} baseline_mean={mean:.1f} z={z:.2f}",
        measured_at=datetime.now(timezone.utc),
    )


def freshness(
    df: pd.DataFrame, table: str, ts_column: str, max_age: timedelta
) -> DQResult:
    """Alert when newest record is older than max_age."""
    if df.empty or ts_column not in df.columns:
        return DQResult(
            check_name="freshness",
            table=table,
            passed=False,
            severity="error",
            message=f"empty or missing column '{ts_column}'",
            measured_at=datetime.now(timezone.utc),
        )

    newest = pd.to_datetime(df[ts_column]).max()
    age = datetime.now(timezone.utc) - newest.to_pydatetime().replace(tzinfo=timezone.utc)
    passed = age <= max_age
    return DQResult(
        check_name="freshness",
        table=table,
        passed=passed,
        severity="error" if not passed else "info",
        message=f"newest={newest} age={age}",
        measured_at=datetime.now(timezone.utc),
    )


def schema_drift(
    current_schema: dict[str, str], expected_schema: dict[str, str], table: str
) -> DQResult:
    """Detect added, removed, or type-changed columns."""
    added = set(current_schema) - set(expected_schema)
    removed = set(expected_schema) - set(current_schema)
    common = set(current_schema) & set(expected_schema)
    type_changed = {c: (expected_schema[c], current_schema[c]) for c in common if expected_schema[c] != current_schema[c]}

    drifted = bool(added or removed or type_changed)
    msg_parts = []
    if added:
        msg_parts.append(f"added={sorted(added)}")
    if removed:
        msg_parts.append(f"removed={sorted(removed)}")
    if type_changed:
        msg_parts.append(f"type_changed={type_changed}")
    message = "; ".join(msg_parts) if msg_parts else "no drift"

    return DQResult(
        check_name="schema_drift",
        table=table,
        passed=not drifted,
        severity="warning" if drifted else "info",
        message=message,
        measured_at=datetime.now(timezone.utc),
    )


def null_rate(df: pd.DataFrame, table: str, max_null_rate: float = 0.1) -> list[DQResult]:
    """Per-column null rate check."""
    results: list[DQResult] = []
    if df.empty:
        return results
    for col in df.columns:
        rate = float(df[col].isna().mean())
        passed = rate <= max_null_rate
        results.append(
            DQResult(
                check_name=f"null_rate[{col}]",
                table=table,
                passed=passed,
                severity="warning" if not passed else "info",
                message=f"null_rate={rate:.3f} threshold={max_null_rate}",
                measured_at=datetime.now(timezone.utc),
            )
        )
    return results
