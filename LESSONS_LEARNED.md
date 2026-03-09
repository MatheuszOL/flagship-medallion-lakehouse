# Trial and Error Notes

Run date: `2026-03-09` on Windows.

Input used in the local test:
`data/raw/sample_taxi.csv`

Command I ran:
`python src/medallion_pipeline.py --input-path data/raw/sample_taxi.csv --input-format csv --sensitive-columns "email,cpf,phone,card_number"`

What happened:
- Spark did not initialize.
- Error raised: `HADOOP_HOME and hadoop.home.dir are unset` (missing `winutils` on Windows).
- Log: `reports/run_2026-03-09_17-53-48.log`

What this taught me in practice:
- Pipeline code can be valid while local runtime fails before any Bronze/Silver/Gold step starts.
- On Windows, Spark + Delta setup is usually the first failure point, not business logic.
- For ingestion-heavy pipelines, schema checks should stay close to the read step because CSV/parquet drift is common.
- Timestamp casting and dedup keys should always be reviewed with real data samples, not assumed from docs.

Companion notes:
`reports/run_log.md`
