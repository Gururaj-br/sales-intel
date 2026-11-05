-- config_checks.sql: fills every "VERIFY" in config/*.yaml. Read-only, part-00 only.
-- Run from repo root:
--   duckdb data/sample/profile.duckdb < docs/queries/config_checks.sql > logs/config_checks.txt 2>&1
-- Laptop-safe: capped memory + threads, and only the ~12 columns we need are copied
-- (the old version copied all 80 incl. huge html/banner text -> 4 GB spill, laptop hung).

SET memory_limit = '4GB';             -- default is ~80% of RAM -> laptop freezes
SET threads = 4;                      -- leave half the CPUs for you
SET preserve_insertion_order = false; -- lets DuckDB stream instead of buffering
SET temp_directory = 'data/sample/duckdb_tmp';

.maxrows 200
.mode markdown
.timer on

-- sample_size only controls schema guessing. 200000 is the verified value (CLAUDE.md):
-- with 20000, rare modules (mongodb, microsoft_exchange) are not detected and the
-- slim table below fails with "column not found".
CREATE OR REPLACE TEMP MACRO raw0() AS TABLE
SELECT * FROM read_json('data/raw/part-00.json.zst',
  format='newline_delimited', sample_size=200000,
  maximum_object_size=33554432, ignore_errors=true);

.print == Q1 all columns and types (schema only, no table built)
DESCRIBE SELECT * FROM raw0();

-- Slim copy: only columns already seen in DATA_PROFILE.md. No html, data, tcp, http.
CREATE TABLE IF NOT EXISTS p0 AS
SELECT port, org, isp, tags, domains, hostnames, cloud, vulns, ssl,
       mongodb, microsoft_exchange, "timestamp"
FROM raw0();

-- Same split as exclusions.yaml: CDN/WAF = hard, clouds = needs attribution.
CREATE OR REPLACE TEMP MACRO is_cdn(o)   AS regexp_matches(lower(coalesce(o,'')), 'incapsula|cloudflare|akamai');
CREATE OR REPLACE TEMP MACRO is_cloud(o) AS regexp_matches(lower(coalesce(o,'')), 'google|amazon|aliyun|alibaba|microsoft');

.print == Q2 tags distribution (honeypot? residential? anything else?)
SELECT t, count(*) AS n FROM (SELECT unnest(tags) AS t FROM p0) GROUP BY t ORDER BY n DESC LIMIT 50;

.print == Q3 cloud field values (shape + providers)
SELECT cloud, count(*) AS n FROM p0 WHERE cloud IS NOT NULL GROUP BY cloud ORDER BY n DESC LIMIT 30;

.print == Q4 top org/isp outside CDN and big clouds (candidate ISPs / hosters)
SELECT org, isp, count(*) AS n FROM p0
WHERE NOT is_cdn(org) AND NOT is_cloud(org)
GROUP BY ALL ORDER BY n DESC LIMIT 60;

.print == Q5 top hostname domains outside CDN (candidate infra_domains)
SELECT d, count(*) AS n FROM (SELECT unnest(domains) AS d FROM p0 WHERE NOT is_cdn(org))
GROUP BY d ORDER BY n DESC LIMIT 60;

.print == Q6 risky ports: how many are on CDN / cloud / other rows
SELECT port, count(*) AS n,
  count(*) FILTER (WHERE is_cdn(org))   AS on_cdn,
  count(*) FILTER (WHERE is_cloud(org)) AS on_cloud,
  count(*) FILTER (WHERE NOT is_cdn(org) AND NOT is_cloud(org)) AS other
FROM p0 WHERE port IN (23,445,1433,3306,3389,5432,5900,5985,6379,9200,27017)
GROUP BY port ORDER BY port;

.print == Q7 one sample of each nested field we want rules on (first 1500 chars)
.mode line
SELECT substr(CAST(ssl AS VARCHAR),1,1500) AS ssl_sample FROM p0
  WHERE ssl IS NOT NULL AND NOT is_cdn(org) AND NOT is_cloud(org) LIMIT 1;
SELECT substr(CAST(mongodb AS VARCHAR),1,1500) AS mongodb_sample FROM p0 WHERE mongodb IS NOT NULL LIMIT 1;
SELECT substr(CAST(microsoft_exchange AS VARCHAR),1,1500) AS exchange_sample FROM p0 WHERE microsoft_exchange IS NOT NULL LIMIT 1;
SELECT substr(CAST(vulns AS VARCHAR),1,1500) AS vulns_sample FROM p0 WHERE vulns IS NOT NULL LIMIT 1;
