# SIGNALS (ideas checked against the data: built, dropped or not built)

| # | Signal | Source fields | Status | Pitch angle |
|---|---|---|---|---|
| 1 | Exposed remote/db services (3389, 1433, 5985, 445, 23, 5900, 3306, 5432, 27017, 6379, 9200) | port, module | **built** (rdp, smb, mssql, winrm, telnet, vnc, mysql, postgres, mongodb, redis, elasticsearch) | Attack surface mgmt / zero trust |
| 2 | Known CVEs | vulns | **built** (max CVSS, +2 if known-exploited) | Vulnerability mgmt |
| 3 | Old Exchange build | microsoft_exchange | **built** (Exchange 2013/2016/2019) | Patching / email security |
| 4 | Unauthenticated DB | mongodb.authentication etc. | dropped: no hits in the data (all MongoDB rows required auth) | Data security |
| 5 | EOL server versions | http.server, banner | **built** via the scanner's eol-product / eol-os tags | Vulnerability mgmt |
| 6 | Weak TLS / expired cert | ssl | **built** (old TLS/SSL versions, expired cert, self-signed cert) | Cert/TLS hygiene |
| 7 | Hygiene gap (no HSTS, no securitytxt) | http.headers, securitytxt | not built | Posture mgmt |
| 8 | Shadow-asset clustering (shared cert/JARM/html hash) | ssl.chain_sha256, jarm, http.html_hash | not built | Asset discovery |
| 9 | Origin exposure behind WAF | cert/page hash on edge + direct IP | not built (needs CDN rows to expose the customer) | Fix WAF bypass |
| 10 | Competitor install base | org = CDN/WAF vendor | not built | Displacement |

Rule per signal: severity, decay half-life, diminishing-returns function live in `config/`.
