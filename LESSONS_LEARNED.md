# Lessons Learned

## Local Run Notes

- Study date: `2026-03-09` (Windows local environment)
- Input used for local validation: `data/raw/sample_taxi.csv`
- Transcript log: `reports/run_2026-03-09_17-53-48.log`
- Run notes: `reports/run_log.md`

Main local blocker: Spark startup failed on Windows due to missing `HADOOP_HOME/winutils`.

## Lessons

- In local Windows runs, Spark + Delta can fail before processing if `winutils` is missing.
- CSV/parquet ingestion is affected by schema drift; validating required columns early avoids late-stage failures.
- Timestamp normalization with `to_timestamp` is sensitive to input format and can affect Silver quality filters.
- Deduplication keys should be reviewed by dataset version to avoid over- or under-deduplication.
