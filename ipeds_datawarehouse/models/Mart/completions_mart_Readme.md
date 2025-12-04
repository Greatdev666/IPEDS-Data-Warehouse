# Gold / Marts — Completions

This document describes the completions marts and metrics created from IPEDS Gold-layer models.

## Models included
- `mart_completions`  
  Aggregated completions (total + male/female) by institution and academic year. Source: `fact_completions_total` joined to `dim_academic_year` and `dim_institutions`. Defensive aggregation used to ensure a single row per institution-year.

- `mart_completions_award_level`  
  Completions pivoted into award-level buckets (associates, bachelors, masters, doctoral types) with a `total_completions` column. Aggregates with `SUM(CASE WHEN ...)` to avoid double-counting.

- `mart_completions_metrics`  
  Derived metrics built from the `mart_completions`/`mart_completions_award_level`:
  - YoY absolute and percent changes
  - Gender share (male/female percent)
  - Cohort-based graduation rates (100% and 150%) where cohort fields exist
  - Transfer-out rate (where cohort info is present)

## Key design choices
- **Defensive aggregation:** all marts aggregate and `GROUP BY` on dimensions to collapse duplicate inputs in source fact tables. This avoids duplicate rows in the mart.
- **Nulls vs 0s:** base numeric facts are `COALESCE(..., 0)` at the Gold layer where appropriate to enable sums; derived rate/YoY fields remain `NULL` when denominator or prior value is 0 or missing.
- **Testing:** uniqueness and fk relationship tests are configured in `schema.yml`. Metric fields are not tested for `NOT NULL` (by design — cohort data may be missing).
- **Materialization:** we recommend materializing these marts as `table` (or `incremental` if you have high data volume) so downstream metrics and BI queries run quickly.

## Notes & next steps
- `mart_completions_award_level`: ensure `dim_award_level` codes match `award_level_code` used in the fact table. If IPEDS uses unexpected codes, document them in `dim_award_level`.
- For cohorts / completion rates: verify mapping between `grtype` codes and cohort/completion semantics (some codes are non-standard or dataset-specific).
- You can build dashboards from `mart_completions_metrics`. Keep metric calculations in the mart metrics model (not in Power BI) for reproducibility and governance.
