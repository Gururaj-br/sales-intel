# DECISIONS LOG (ADR-lite)
Format: date | decision | why | alternatives rejected

- D1 | No Spark | one 10 GB part fits a laptop with DuckDB; hosts can't run Spark | Spark, Airflow
- D2 | Exclude CDN/WAF/Google edge before scoring | ~60% of rows are vendor infra | scoring everything
- D3 | Abstain over guess | a wrong lead costs a rep more than a missed one | fill-in scores
- D4 | Ranking is deterministic, LLM only validated side-steps | auditability | LLM scoring
- D5 | Rules in YAML | visible, tunable, reviewable | hardcoded
- D6 | The raw banner text and other fields no rule uses (JARM, cert hash, CPE, HTTP server, login flags) are not copied to silver; they stay in bronze and can be added later | smaller silver (about 40% less), less noise | parsing the full text into silver
- D7 | One LLM step only: label owner names (company / infrastructure / default / unknown), offline, cached, checked on a gold set | hand lists of hosters and device makers stopped converging; names are few and repeat across files, so cost stays low | LLM scoring, LLM in the hosted app
- D8 | Files land in landing/dt=…/source=…/; split into 500k-record parts under bronze|silver/…/dt=…/source=…/file=…/ (Hive), same keys locally and in S3; `hr=` only as a lake-wide switch | flat memory, parallel and restartable workers | one process per whole file, Spark