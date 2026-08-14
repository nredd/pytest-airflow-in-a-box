"""Render a plain-text migration diff report.

No ANSI or tty assumptions -- `doctor.py`'s bullet-report style, safe for a redirected
file or a CI log.

References:
    https://github.com/nredd/pytest-airflow-in-a-box/issues/42
    https://github.com/nredd/pytest-airflow-in-a-box/issues/44
"""

from __future__ import annotations

from pytest_airflow_in_a_box.migration.provision import ProvisionedEnvironment
from pytest_airflow_in_a_box.migration.types import DiffResult

REPORT_TITLE = "pytest-airflow-in-a-box migration diff"


def _environment_lines(
    baseline_env: ProvisionedEnvironment, live_env: ProvisionedEnvironment
) -> list[str]:
    """Render the two provisioned environments' identifying details.

    Parameters:
        baseline_env: ProvisionedEnvironment containing the recorded 2.x environment.
        live_env: ProvisionedEnvironment containing the recorded 3.x environment.

    Returns:
        list[str] containing report bullet lines.
    """

    return [
        f"- Baseline: Airflow {baseline_env.airflow_version} ({baseline_env.family.name}) "
        f"on Python {baseline_env.python_version}",
        f"- Live: Airflow {live_env.airflow_version} ({live_env.family.name}) "
        f"on Python {live_env.python_version}",
    ]


def _counts_lines(diff: DiffResult) -> list[str]:
    """Render all seven category counts.

    Parameters:
        diff: DiffResult containing the categorized migration diff.

    Returns:
        list[str] containing report bullet lines.
    """

    counts = diff.counts
    return [
        f"- Regression: {counts.regression}",
        f"- Fixed: {counts.fixed}",
        f"- Broken on both: {counts.broken_on_both}",
        f"- Still passing: {counts.still_passing}",
        f"- Gated: {counts.gated}",
        f"- New: {counts.new}",
        f"- Missing: {counts.missing}",
    ]


def _nodeid_section(title: str, nodeids: tuple[str, ...]) -> list[str]:
    """Render one nodeid list section, or an explicit empty-list line.

    Parameters:
        title: str naming the section.
        nodeids: tuple[str, ...] containing the nodeids to list.

    Returns:
        list[str] containing a section header line followed by nodeid bullets.
    """

    lines = [f"## {title}"]
    if not nodeids:
        lines.append("- (none)")
        return lines
    lines.extend(f"- `{nodeid}`" for nodeid in nodeids)
    return lines


def render_diff(
    diff: DiffResult,
    *,
    baseline_env: ProvisionedEnvironment,
    live_env: ProvisionedEnvironment,
) -> str:
    """Render a complete plain-text migration diff report.

    Parameters:
        diff: DiffResult containing the categorized migration diff.
        baseline_env: ProvisionedEnvironment containing the recorded 2.x environment.
        live_env: ProvisionedEnvironment containing the recorded 3.x environment.

    Returns:
        str containing the complete report, newline-terminated.
    """

    lines = [REPORT_TITLE, ""]
    lines.extend(_environment_lines(baseline_env, live_env))
    lines.append("")
    lines.append("## Counts")
    lines.extend(_counts_lines(diff))
    lines.append("")
    lines.extend(_nodeid_section("Regressions", diff.regression_nodeids))
    lines.append("")
    lines.extend(_nodeid_section("Fixed", diff.fixed_nodeids))
    lines.append("")
    lines.extend(_nodeid_section("New", diff.new_nodeids))
    lines.append("")
    lines.extend(_nodeid_section("Missing", diff.missing_nodeids))
    if diff.warnings:
        lines.append("")
        lines.append("## Warnings")
        lines.extend(f"- {warning}" for warning in diff.warnings)
    lines.append("")
    lines.append("Result: REGRESSIONS FOUND" if diff.has_regressions else "Result: no regressions")
    return "\n".join(lines).rstrip() + "\n"


__all__ = ("REPORT_TITLE", "render_diff")
