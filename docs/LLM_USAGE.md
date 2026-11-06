# LLM USAGE & COST

| Step | Model | Input unit | Calls | Tokens in/out | $ per 1k units | Cache hit | Accuracy vs gold |
|---|---|---|---|---|---|---|---|
| Org classification (prompt v1) | Groq openai/gpt-oss-120b | owner name of would-be-ranked account | 20 (40 names/call) | 25,350 / 38,356 | ~$0.06 per 1k names (VERIFY price) | 0% first run | drop precision 0.97, recall 0.70, exact 0.83 on 96 gold names (provisional) |
| Org classification (prompt v2, KEPT) | Groq openai/gpt-oss-120b | owner name of would-be-ranked account | 20 (40 names/call) | 29,450 / 42,150 | ~$0.07 per 1k names (VERIFY price) | 0% (new prompt version) | drop precision 0.94, recall 0.77, exact 0.87 on 96 gold names (provisional; 0.85 before the 6 retried names) |
| Ask AI tab (optional, config switch; answer prompt v2) | Groq openai/gpt-oss-120b | one rep question | 2 per question (tool choice + answer) | not measured yet | VERIFY | cached per question while the app runs | tool choice on 10 gold questions: not run yet (`eval/eval_ask.py`) |
| Gold pre-labeling | Claude (in the coding session) | gold name | n/a | n/a | n/a | n/a | drafts only; human review pending |

Planned and **not built**: LLM drafting of finding rules for rare modules, LLM help on ambiguous ownership.
Scope of the numbers above: owner names from the first 500,000 records of the scan file.

Guardrails: JSON schema validation, temperature 0, "unknown" fallback, cache by content hash, prompts versioned in `prompts/`.
Rule: if LLM does not beat the regex baseline on the gold set, remove it and say so.
Hosted app: rankings use no LLM at view time. The optional Ask AI tab does (2 calls per question), with the key in
Streamlit secrets and at most 10 new questions per browser session.

Rate limit: the Groq free tier allows 8,000 tokens per minute for this model and one call of 40 names uses
about 3,600 tokens (about 1,500 in, 2,100 out, mostly the model's reasoning), so about 2 calls fit in a minute.
`classify.py` paces calls to stay under `tokens_per_minute` (config/llm.yaml) and, if a 429 still comes back,
waits as long as its retry-after header says. A full run of 772 new names takes about 10 minutes; cached names cost nothing.
All keys of one Groq organization share that limit, so rotating keys does not help.

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
- 2026-11-05 retry run: the 6 unanswered names got labels (1 call). Now company 450 / default 194 / infrastructure 82 /
  unknown 46; 276 names dropped (was 274). Ranked accounts 490 -> 489, exact label match 0.854 -> 0.865, recall and precision unchanged.
