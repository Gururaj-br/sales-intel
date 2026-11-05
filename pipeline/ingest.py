"""ingest.py: bronze -> silver. One raw part (part-XX.json.zst) -> one slim Parquet file.

What it does, per part:
  1. Read only the JSON keys we need (explicit column list = no schema guessing, so a
     part where a rare module is missing still works; the column is just NULL).
  2. Flatten them into plain columns (cert org, max CVSS, TLS versions, ...).
  3. Keep one row per ip + port + transport (latest timestamp wins).
  4. Check the output with pandera and write Parquet.

It does NOT decide who is a prospect or what is a finding; later stages do that.

Run by orchestrator/process.py, one part file at a time (python -m orchestrator ...).
"""
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandera.polars as pa
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import scalar  # noqa: E402

# Fields read from each raw record, with their types. Nested values are read as STRUCTs that
# name ONLY the sub-fields we need: DuckDB then skips everything else while parsing
# (http.html, the PEM cert chain, CVE summaries, the raw banner...). Every field
# that no rule uses is left out: it stays in bronze and can be added back here any time. Reading whole nested values as JSON
# text ran out of memory on a full part. Missing keys become NULL; extra keys are ignored.
# Every field here was seen in DESCRIBE on part-00 (docs/DATA_PROFILE.md).
RAW_COLUMNS = {
    "ip_str": "VARCHAR", "ipv6": "VARCHAR", "port": "BIGINT", "transport": "VARCHAR",
    "timestamp": "VARCHAR", "org": "VARCHAR",
    "location": "STRUCT(country_code VARCHAR)",
    "hostnames": "VARCHAR[]", "domains": "VARCHAR[]", "tags": "VARCHAR[]",
    "cloud": "STRUCT(provider VARCHAR, service VARCHAR)",
    "_shodan": "STRUCT(module VARCHAR)",
    "product": "VARCHAR",
    "ssl": "STRUCT(versions VARCHAR[], cert STRUCT(expired BOOLEAN, subject STRUCT(CN VARCHAR, O VARCHAR)))",
    "vulns": "MAP(VARCHAR, STRUCT(cvss DOUBLE, kev JSON))",   # cve id -> details
    "microsoft_exchange": "STRUCT(name VARCHAR)",
}

SELECT_SQL = f"""
SELECT
  coalesce(ip_str, ipv6) AS ip_str,                       -- IPv6 records keep ip_str empty
  ip_str IS NULL AND ipv6 IS NOT NULL                      AS is_ipv6,
  port, transport,
  TRY_CAST("timestamp" AS TIMESTAMP)                       AS ts,
  org,
  location.country_code                                    AS country,
  hostnames, domains, tags,
  cloud.provider                                           AS cloud_provider,
  cloud.service                                            AS cloud_service,
  _shodan.module                                           AS module,
  product,
  ssl.cert.subject.CN                                      AS cert_cn,
  ssl.cert.subject.O                                       AS cert_o,
  ssl.cert.expired                                         AS cert_expired,
  ssl.versions                                             AS ssl_versions,
  map_keys(vulns)                                          AS cve_ids,
  list_max(list_transform(map_values(vulns), lambda v: v.cvss))              AS cve_max_cvss,
  len(list_filter(map_values(vulns), lambda v: CAST(v.kev AS VARCHAR) = 'true')) AS cve_n_kev,
  microsoft_exchange.name                                  AS exchange_name
FROM read_json('@path@', format = '@format@', columns = {RAW_COLUMNS!r},
               maximum_object_size = @max_obj@, ignore_errors = @ignore_errors@)
"""

# Cheap second read: just count records. If a typed field above does not fit a record,
# ignore_errors may drop that record; comparing the two counts makes such loss visible.
COUNT_SQL = """
SELECT count(*) FROM read_json('@path@', format = '@format@', columns = {'port': 'BIGINT'},
                               maximum_object_size = @max_obj@, ignore_errors = @ignore_errors@)
"""

# Output contract. Later stages may rely on exactly these guarantees.
SCHEMA = pa.DataFrameSchema(
    {
        "part": pa.Column(str, nullable=False),
        "ip_str": pa.Column(str, nullable=False),
        "port": pa.Column(pl.Int64, pa.Check.in_range(0, 65535), nullable=False),
        "ts": pa.Column(pl.Datetime, nullable=True),
        "cve_max_cvss": pa.Column(float, pa.Check.in_range(0, 10), nullable=True),
    },
    unique=["ip_str", "port", "transport"],
    strict=False,  # other columns allowed; these are the ones we promise
)


def part_name(path: Path) -> str:
    return path.name.split(".")[0]  # part-00.json.zst -> part-00


def ingest_part(con, raw: Path, out_dir: Path, cfg: dict) -> dict:
    part = part_name(raw)
    out = out_dir / f"{part}.parquet"
    rj = cfg["read_json"]
    t0 = time.time()

    values = {
        "@path@": str(raw).replace("'", "''"), "@format@": rj["format"],
        "@max_obj@": str(int(rj["maximum_object_size"])),
        "@ignore_errors@": str(bool(rj["ignore_errors"])).lower(),
    }
    def fill(template: str) -> str:
        for token, value in values.items():
            template = template.replace(token, value)
        return template

    rows_raw = scalar(con, fill(COUNT_SQL))
    con.sql(f"CREATE OR REPLACE TEMP TABLE obs AS {fill(SELECT_SQL)}")
    rows_in = scalar(con, "SELECT count(*) FROM obs")
    no_ip = scalar(con, "SELECT count(*) FROM obs WHERE ip_str IS NULL OR port IS NULL")

    # One row per ip + port + transport inside this part: latest scan wins.
    df = con.sql(f"""
        SELECT '{part}' AS part, * FROM obs
        WHERE ip_str IS NOT NULL AND port IS NOT NULL
        QUALIFY row_number() OVER (PARTITION BY ip_str, port, transport
                                   ORDER BY ts DESC NULLS LAST) = 1
    """).pl()
    SCHEMA.validate(df)  # raises with a clear message if a promise is broken

    df.write_parquet(out, compression="zstd")
    con.sql("DROP TABLE obs")

    return {
        "part": part, "status": "ok", "rows_raw": rows_raw, "rows_rejected": rows_raw - rows_in,
        "rows_in": rows_in, "rows_no_ip": no_ip,
        "rows_out": df.height, "dupes_dropped": rows_in - no_ip - df.height,
        "seconds": round(time.time() - t0, 1),
        "finished_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
