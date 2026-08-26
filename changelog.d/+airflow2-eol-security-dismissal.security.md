Document in `SECURITY.md` that Dependabot alerts against the `airflow2` extra's
EOL `apache-airflow==2.11.2` resolution (and its unpatchable `flask`/`flask-appbuilder`/
`apache-airflow-providers-fab` pins) are dismissed as tolerable risk, since this plugin
only runs Airflow's webserver as an ephemeral local test harness.
