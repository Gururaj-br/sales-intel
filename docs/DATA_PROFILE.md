# DATA PROFILE (part-00, 500,000 rows)

| Metric | Value |
|---|---|
| rows | 500,000 |
| distinct IPs | 319,393 |
| distinct org strings | 14,592 |
| rows with vulns | 10,671 (~2%) |
| rows with ssl | 41,083 (~8%) |
| rows with http | 125,458 (~25%) |
| rows with cloud | 117,947 (~24%) |

## Top orgs (rows)
Incapsula Inc 151,161 (+ "Incapsula Inc." 2,623) | Google LLC 93,823 | Cloudflare 60,173 | Aliyun 13,490 (+4,083 variant)
| Amazon.com 8,480 | Amazon Technologies 6,482 | ACE 3,959 | Internet Rimon 3,499 | Alibaba Cloud 3,257 | Microsoft 3,057 | Akamai 2,766 ...
Name variants (punctuation/case/suffix) must be normalized: "ServiceNow, Inc." vs "SERVICENOW, INC.".

## Top ports
2087 (64k), 443 (30k), 80 (23k), 541, 8069, 8889, 1433 (10k MSSQL), 5997, 1957, 5985 (7k WinRM), 8000, 5275, 5241, 9129, 49, 25 ...
Odd ports (2087, 8069, 8889, 5997, 1957, 5275, 5241, 9129) are owned by Incapsula/Cloudflare/Google = proxy noise.

## Non-CDN rows (excluding incapsula|cloudflare|akamai regex; Google still included)
- total 282,804 | has_domain 182,013 | has_hostname 182,013 | has_ssl 28,810

## Columns (80; partial list seen)
data, opts, port, hostnames, timestamp, domains, location, org, isp, os, http, tags, tcp, _shodan, ssl, cloud, hash, transport, ipv6, asn,
... rdp_encryption, dav, milesight_routers, iscsi, plex, ldap, steam_ihs, ftp, neo4j_browser, upnp, mongodb, mikrotik_routeros, title, html,
mdns, pptp, coap, handpunch, mcp, microsoft_exchange
(paste full list here after `.maxrows 100` + DESCRIBE)

## Still unverified
- Do parts differ (random split vs sorted)? Compare part-13, part-26.
- Top non-CDN domains (infra blocklist), domains on CDN rows, cert JSON structure, tags distribution.
- Attributable account count after blocklist.

## Verified 2026-11-04 (docs/queries/config_checks.sql -> logs/config_checks.txt)
- Schema with sample_size=200000: **120 columns** (not 80; rare modules appear only with the big sample).
- `tags` (VARCHAR[]): cdn 226,049 | cloud 117,912 | pqc 11,280 | honeypot 8,635 | vpn 5,451 | proxy 4,809 |
  self-signed 4,099 | eol-product 3,801 | eol-os 2,183 | database 559 | iot 282 ... `cdn` tag catches 8,980 rows the org regex misses.
- `cloud` = STRUCT(region, service, provider). Providers: Google, Amazon, Alibaba Cloud, Tencent, DigitalOcean, Azure, Linode, Oracle.
  service CLOUDFRONT (7,147) / GLOBALACCELERATOR (595) = edge, not customer servers.
- Risky ports: 1433 has 10,287 rows but **9,693 on CDN** (proxy noise). 5985 (7,188) is mostly Cogent/Tencent/Hetzner.
  Real counts of 3389/445/23/... are tiny (30-110 rows each).
- `ssl.cert.subject` is a **MAP** (use `subject['O'][1]`). Off-CDN certs: 25,080; with O: 10,423.
  Top cert O values are factory defaults: Fortinet 2,693, Huawei, "Default Company Ltd", "Acme Co" = not owners.
- `ssl.versions` list: "-TLSv1" means not supported; "TLSv1" means supported. ssl.cert.expired = 1,732 off-CDN.
- `vulns` = MAP(cve -> {cvss, epss, kev, verified, ...}); sample had verified=false.
- `microsoft_exchange.name`: 29 rows; mostly 2013/2016/2019 (out of support), 3 on "Exchange Server SE".
- `mongodb`: 11 rows, all authentication=true -> unauth-Mongo rule has no hits.
- Real companies appear as `org` with isp = Google/Cloudflare (ServiceNow, Walmart, Palo Alto Networks, Fiserv, Egnyte, HubSpot).

## After ingest (snapshot from 2026-11-04; today the same rows are in silver/observations of the lake)
- 500,000 raw -> 0 rejected -> 19 without ip/port -> 36,490 duplicates (7.3%, same ip+port+transport) -> **463,491 rows**, 24 MB, 12.6 s (13 MB since unused fields stay in bronze, D6).
- 323,217 distinct IPs, of which 4,466 rows are IPv6 (Shodan puts them in `ipv6`, not `ip_str`).
- Timestamps span only **~1 hour** (2026-11-01 00:10 -> 01:08). Recency decay is meaningless inside one part.
- After dedupe: cve rows 10,387 (5,739 with a known-exploited CVE), cert 37,412 (cert O 11,982), exchange 26.
- No-login flags are near-empty: ftp anonymous 3, rsync no auth 5, redis/clickhouse 0.

## After eligibility (part-00, 463,491 rows, 2026-11-04)
| status / reason | round 1 | round 2 (lists extended) |
|---|---:|---:|
| excluded / cdn_waf | 199,593 | 200,020 |
| needs_attribution / cloud_hosting | 136,786 | 138,813 |
| **eligible** | **93,722 (20.2%)** | **79,828 (17.2%)** |
| excluded / isp | 23,565 | 35,005 |
| excluded / honeypot | 8,583 | 8,583 |
| excluded / security_vendor | 1,242 | 1,242 |
- Security-vendor hits include cisco.com / trendmicro.com servers hosted on AWS: correct (vendor-owned).
- Eligible is a **long tail**: 11,155 of 12,990 eligible orgs have 1-5 rows; top 200 orgs = 53% of rows.
  Hand lists cannot cover it -> LLM org classifier (docs/DECISIONS.md D7) is needed, with a gold set.
- Round 3: unknown big orgs moved to needs_attribution / unclassified_org (exact name match): 14,560 rows ->
  **eligible 79,828 -> 65,268 (14.1%)**. List below: ACE / ACEVILLE PTE.LTD. (~6.3k rows),
  Meteverse Limited (~4.2k), LUOGELANG (FRANCE) LIMITED, eSited Solutions, Internet Utilities NA LLC,
  CloudIP LLC, "End User Organization", Aviation RE LLC, AviationAI, Quality Technology Services.

## After attribution (part-00, 2026-11-04)
Eligibility rerun first (Zscaler, Proofpoint, iboss, ddos-guard added): eligible 65,268 -> **64,843**; security_vendor 1,242 -> 1,656.

| status | tier A cert | tier B domain | tier C org | D (no owner) |
|---|---:|---:|---:|---:|
| eligible (64,843) | 1,957 | 10,995 | 51,866 | 25 |
| needs_attribution (153,373) | 838 | 6,309 | never | 146,226 (598 = several domains) |
| excluded (245,275) | - | - | - | all |

- Owners: A 1,517 / B 11,549 / C 9,854 distinct keys. Only **4.7%** of cloud rows (7,147) get a proven owner; the rest abstain.
- Reverse-DNS rule (hostname holds the IP as numbers, hex, or an ISP word) removes most ISP/hoster hostnames
  without a list: 126k of 192k hostnames matched even the first, narrow version.
- **Precision by eyeball, top-40 owners (not a gold set):** tier A ~27/40 real (rest = device makers, codes like
  "dis", "qy"); tier B ~18/40 real, ~12/40 still hosters/ISPs (hostgator, stratoserver, bbtec.net), 10 unclear.
  Tier B is far below the 0.9 precision target; its 0.7 weight in scoring.yaml is too high until the LLM
  domain classifier + gold set exist.
- Tier C top orgs still include ISPs from the long tail (Sky UK, Bharti Airtel, Reliance Jio, Hurricane Electric,
  "IP Pools", "Private Customer") -> LLM org classifier.
- Same company can get different keys by tier (servicenow vs service-now.com). Merging is a later step.
- Runtime: 4.8 s, 1.3 GB peak on part-00.
- **Weights changed** (user decision): B 0.7 -> 0.5, plus agreement bonus +0.3 when two independent proofs
  (cert O / domain / org) name the same company. Rows that can pass abstain (weight x best evidence 0.9 >= 0.5):

  | | rows | owners |
  |---|---:|---:|
  | before (B 0.7) | 20,099 (A + B) | |
  | B 0.5, no bonus | 2,795 (A only) | |
  | **B 0.5 + agreement** | **3,896** (A 2,795 + agreed B 1,101) | agreed: 285 + 130 A, 569 B |
- Agreeing B rows take the org's key (service-now.com -> servicenow), so one company = one account.
- Top-40 agreeing owners: ~20 real prospects, ~20 hosters/ISPs (HostGator, Telstra, Beget, Bell Canada):
  their org is correctly the owner but should not be eligible -> eligibility / org classifier gap.
- **Round 4, hosting/ISP words** (hosting, host, server, datacenter, data center, vps, colocation, dedicated,
  cloud computing/service -> needs_attribution; broadband, isp -> excluded):
  eligible 64,843 -> **60,796**; rows able to pass abstain 3,896 -> **3,719** (2,064 agreed rows, 884 owners);
  top-40 agreeing owners ~24/40 real. Left: ISPs/hosters with no generic word (Telstra, Beget, Infomaniak,
  Bell Canada, Swisscom) and in-word hits (BisectHosting) -> LLM org classifier.

## After findings + score (part-00, 2026-11-04) = BASELINE before any LLM step
- findings.py: **15,262** finding rows (0.4 s). has_cve 3,763 | winrm_http 3,257 | old_tls 2,214 | eol_product 2,003 |
  self_signed 1,867 | expired_cert 1,156 | eol_os 706 | mssql 180 | mysql 30 | rdp 23 | old_exchange 17 | smb 13 | rest < 10.
  winrm_http is real (product = WinRM) but mostly on long-tail hosters/ISPs (Hongkong Service, Saigon Postel, Zillion Network).
- score.py: **7,081 accounts -> 753 ranked, 6,328 abstain** (confidence < 0.5).
- **Baseline precision, top-25 ranked, by eyeball (not a gold set): ~12/25 real prospects** (Broadcom, Medidata,
  Oracle, VMware, Baidu, Univ. of Florida, McMaster, SERPRO, PRODESP, ExpressVPN, Spice Shed, UFRGS).
  ~10/25 are software/device default certs taken as owners (Bitnami, LaCie, Issabel, Bitrix, CentOS Web Panel,
  "Axentraserver Default Certificate", "IT"), ~3/25 hosters/ISPs (Link3, KAGOYA, Free SAS).
  -> The LLM step must classify cert O values as well as org names. This number is what it has to beat.
