# Pipeline Run Log

## Run Metadata

- Date: 2026-03-09
- Pipeline: Bronze -> Silver -> Gold
- Engine: PySpark + Delta Lake
- Dataset: NYC Taxi (public source, parquet file in `data/raw/`)

## Engineering Notes

- Bronze persisted raw records without business transformation.
- Silver enforced null filtering, timestamp casting, deduplication, and PII anonymization.
- Gold generated business-facing aggregates for KPI consumption.

## Governance Notes

- Sensitive fields are anonymized with salted SHA-256 hashing.
- Curated layers are suitable for analytics while reducing exposure of direct identifiers.

## Outcome

- Pipeline structure validated and scripts compiled.
- Repository is ready for execution with real volume in Spark runtime.
