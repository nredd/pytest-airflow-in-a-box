# Security Policy

## Supported Versions

Before the first stable release, security fixes are provided only on the latest release line.

## Reporting

Report vulnerabilities through GitHub's private vulnerability reporting for
`nredd/pytest-airflow-in-a-box`. Do not open a public issue or include secrets, credentials, or
private Airflow configuration in a report.

Include the affected version, impact, reproduction steps, and any known mitigation. A maintainer
will acknowledge the report and coordinate disclosure after a fix is available.

## Airflow 2.x dependency alerts

The `airflow2` extra intentionally locks `apache-airflow==2.11.2`, the final release
Airflow ever cut on the 2.x line. It is EOL: no further security fixes will ship for it,
its bundled `apache-airflow-providers-fab==1.5.4`, or that provider's hard-pinned
`flask`/`flask-appbuilder`. Verified via `uv lock --dry-run --upgrade-package ...`
(no lockfile changes possible) and PyPI metadata (`apache-airflow-providers-fab==1.5.4`
pins `flask-appbuilder==4.5.4` exactly and `flask<3,>=2.2`, both below every patched
version on record).

Dependabot alerts against this resolution are dismissed as tolerable risk: this plugin
never runs Airflow's webserver in a production or multi-tenant posture, only as an
ephemeral, local, single-user test harness, so auth-bypass/XSS/info-disclosure CVEs in
Airflow's own UI don't apply to how it's used here. `.github/dependabot.yml` already
`ignore`s `apache-airflow*` for version-update PRs for the same reason -- keeping the
2.x floor is required for the plugin's own compat matrix (see issues #25, #139), not an
oversight. These alerts stay dismissed unless the `airflow2` extra's floor moves.
