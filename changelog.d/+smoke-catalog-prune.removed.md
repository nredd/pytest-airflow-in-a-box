Remove the `test_no_duplicate_dag_ids` and `test_dag_parse_budget` bundled `--airflow-smoke`
checks. Both duplicated data `test_dag_bag_integrity` already reports -- a duplicate `dag_id`
collision already surfaces as an ordinary import error, and the parse-budget threshold
re-walked the same per-file parse durations as the existing slowpoke-ratio warning.
