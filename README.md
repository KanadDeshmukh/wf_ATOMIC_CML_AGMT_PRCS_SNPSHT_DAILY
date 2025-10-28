# cml_agmt_proc_snpsht dbt Project

## Overview
Implements end-to-end Informatica ETL/mapping for `CML_AGMT_PROC_SNPSHT` in dbt on Databricks. Replicates all enrichments, lookups, business rules, and pre/post-processing orchestration steps. Staging models isolate raw data; the final mart implements all chained logic. Orchestration (pre-truncate, build, postprocess) is Databricks/Workflow-managed. See models and schema.yml for documentation of all logic and fields.

## Workflow Summary

1. Preprocess: Runs a macro to TRUNCATE the target mart before restart. Fails if not successful.
2. dbt build: Builds all staging models and `mart_cml_agmt_proc_snpsht`. All logic, enrichments, and lookups as per Informatica mapping are implemented here.
3. Postprocess: Dummy/logging macro runs for notification or audit logging at workflow end.

See the workflow diagram in documentation for full lineage.

## Dependencies
- Databricks Unity Catalog (ideal; supports all features)
- dbt-databricks plugin (>=1.2)
- All required staging tables and reference tables loaded in raw schema

## Business Purpose
- Produces a daily point-in-time processed snapshot for downstream reporting, financials, and actuarial feeds, with all fields and groupings per actuarial and finance requirements.
