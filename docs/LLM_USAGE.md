# LLM USAGE & COST

| Step | Model | Input unit | Calls | Tokens in/out | $ per 1k units | Cache hit | Accuracy vs gold |
|---|---|---|---|---|---|---|---|
| Org classification (prompt v1) | Groq openai/gpt-oss-120b | owner name of would-be-ranked account | 20 (40 names/call) | 25,350 / 38,356 | ~$0.06 per 1k names (VERIFY price) | 0% first run | drop precision 0.97, recall 0.70, exact 0.83 on 96 gold names (provisional) |
| Org classification (prompt v2, KEPT) | Groq openai/gpt-oss-120b | owner name of would-be-ranked account | 20 (40 names/call) | 29,450 / 42,150 | ~$0.07 per 1k names (VERIFY price) | 0% (new prompt version) | drop precision 0.94, recall 0.77, exact 0.85 on 96 gold names (provisional) |
| Gold pre-labeling | Claude (in the coding session) | gold name | n/a | n/a | n/a | n/a | drafts only; human review pending |

Planned and **not built**: LLM drafting of finding rules for rare modules, LLM help on ambiguous ownership.
Scope of the numbers above: owner names from the first 500,000 records of the scan file.

Guardrails: JSON schema validation, temperature 0, "unknown" fallback, cache by content hash, prompts versioned in `prompts/`.
Rule: if LLM does not beat the regex baseline on the gold set, remove it and say so.
Hosted app makes **no** LLM calls by default. Optional "Ask" chat: only if config/app.yaml chat.enabled is true.

## Run notes
- 2026-11-04 v1: 772 names -> company 473 / default 132 / infrastructure 77 / unknown 90
  (40 of the 90 = one batch whose answer failed the JSON check; not cached, retried next run).
- First attempt with an older Groq model failed: "This model does not support JSON output"
  (response_format) -> switched to openai/gpt-oss-120b. Cost in logs/llm_cost.csv uses config prices: VERIFY.
- v1 caveat: its prompt examples included 3 gold names (Bitnami, IT, Axentraserver Default Certificate), so v1 gold
  numbers are slightly optimistic. v2 prompt examples are checked to contain no gold names.
- 2026-11-05 v2: adds the "maker name in a cert = default" rule (LaCie NAS, ESXi, iDRAC were labelled company in v1).
  v1 labels kept in data/llm/org_labels_v1.parquet for comparison.
- 2026-11-05 v2 run: company 446 / default 192 / infrastructure 82 / unknown 52; 146 of 772 labels changed vs v1.
  Fixed: LaCie, VMware, Dell -> default. Still wrong: Bitnami, Bitrix, VIMAR, V2Ray (company), SITA dropped as infrastructure.
  One 429 rate limit (Groq free tier 8k tokens/min); retry recovered, 6 names unanswered -> unknown, retried next run.
