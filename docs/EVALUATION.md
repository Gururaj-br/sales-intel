# EVALUATION

Scope: the first 500,000 records of the scan file (one of 27 chunks). Script: `eval/eval_orgs.py`.

## Honest limits
- Single-day data and no sales outcomes, so there is no time-based holdout and no conversion metric.
- One gold set only (owner names). Gold sets for ownership proof and for finding correctness were planned and **not built**.
- The gold labels were drafted with AI help and **have not yet been reviewed by a person** (`reviewed=no` in the file;
  the script prints a warning while any row is unreviewed). All numbers below are provisional.
- Confidence is computed per account from its evidence (owner-proof weight × evidence strength), but it is **not calibrated**
  against outcomes; the weights are starting values in `config/scoring.yaml`.

## Gold set: owner names (`eval/gold_orgs.csv`)
| | |
|---|---|
| Size | 110 names: 70 from the top 60 accounts of the rules-only ranking (some accounts have several name variants) + 40 random names from the rest of that ranking (ranks 61 to 753) |
| Labels | company / infrastructure / default / unknown (same definitions as the LLM prompt) |
| Used in scores | 96 names (the 14 labelled "unknown" are left out) |
| Leak check | LLM prompt examples are checked against gold names. Prompt v1 contained 3 gold names (so v1 is slightly optimistic); v2 contains none |

## Results
| Method | Top-25 real companies | Exact label match | Drop recall | Drop precision |
|---|---|---|---|---|
| Rules only | 10 / 25 | 0.55 | n/a | n/a |
| Rules + LLM prompt v1 (3 gold names leaked) | 14 / 25 | 0.83 | 0.70 | 0.97 |
| **Rules + LLM prompt v2 (kept)** | **15 / 25** | **0.85** | **0.77** | **0.94** |

- *Top-25 real companies*: of the 25 highest-ranked accounts, how many have gold label "company".
- *Drop recall*: of the gold names that are infrastructure/default, the share the LLM removed.
- *Drop precision*: of the names the LLM removed, the share that really were infrastructure/default.
  v2's two wrong removals: "Pusat Data Dan Teknologi Informasi" and "SITA".
- Remaining misses in the v2 top 25: software or device makers labelled company (Bitnami, Bitrix, VIMAR, V2Ray).

## Funnel (same data)
500,000 records → 463,491 unique IP + port → 15,262 problems on owned servers → 14,690 after the LLM name check
→ 6,819 accounts → 490 ranked, 6,329 "not enough proof".

## Not done
- Coverage-vs-precision curve for the abstain threshold (0.5 is a starting value).
- Confidence calibration (reliability diagram).
- A proxy test (predict "has known vulnerabilities" without the vulnerability field).
