Default `airflow_forbid_catchup` and `airflow_forbid_unbounded_expand` to `false`. Both are
now opt-in, like the bundled catalog's other policy checks, instead of binding every consumer
to a stance on `catchup`/`expand()` conventions their team may not share.
