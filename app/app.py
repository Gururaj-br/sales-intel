"""Sales intel app: reads ONLY data/out/*.parquet (precomputed). No LLM calls, no API keys.
(Exception, off by default: the "Ask" tab in config/ask.yaml, for local use with your own key.)

Confidence is computed here, live, from the evidence rows of each account:
  confidence = mean owner-proof weight of its finding rows * mean evidence strength of its findings
The slider moves the abstain line; accounts below it show "insufficient evidence", never a score.

Run:  streamlit run app/app.py
"""
from pathlib import Path

import polars as pl
import streamlit as st
import yaml

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "out"
SCORING = yaml.safe_load((ROOT / "config" / "scoring.yaml").read_text())

EXPLAIN = yaml.safe_load((ROOT / "config" / "explain.yaml").read_text())   # plain-language text
ASK = yaml.safe_load((ROOT / "config" / "ask.yaml").read_text())           # optional Ask tab, off by default


def word(bands: list[dict], value: float) -> str:
    """First band whose min <= value, e.g. severity 9 -> 'critical', confidence 0.7 -> 'Medium'."""
    return next(b["word"] for b in bands if value >= b["min"])


st.set_page_config(page_title="Exposure-led prospecting", layout="wide")


@st.cache_data
def load() -> tuple[pl.DataFrame, pl.DataFrame]:
    acc = pl.read_parquet(OUT / "accounts.parquet")
    fnd = pl.read_parquet(OUT / "account_findings.parquet")
    return acc, fnd


def live_confidence(acc: pl.DataFrame, fnd: pl.DataFrame, threshold: float) -> pl.DataFrame:
    """Recompute confidence from the evidence rows (not the stored column) and apply the slider."""
    conf = fnd.group_by("owner_key").agg(
        pl.col("attr_weight").mean().alias("owner_proof"),
        pl.col("evidence").mean().alias("evidence"),
        pl.col("finding").unique().alias("finding_types"),
        pl.col("attr_agree").any().alias("proofs_agree"),
    ).with_columns((pl.col("owner_proof") * pl.col("evidence")).alias("confidence"))
    return (acc.drop("confidence", "status")
            .join(conf, on="owner_key", how="inner")
            .with_columns((pl.col("confidence") >= threshold).alias("ranked")))


acc, fnd = load()

# ---------------- sidebar: filters ----------------
st.sidebar.header("Filters")
threshold = st.sidebar.slider("Minimum confidence", 0.0, 1.0, float(SCORING["abstain_below"]), 0.05,
                              help="Below this, an account is shown as 'insufficient evidence' and not ranked.")
data = live_confidence(acc, fnd, threshold)
countries = st.sidebar.multiselect("Country", sorted(data["country"].drop_nulls().unique().to_list()))
types = st.sidebar.multiselect("Finding type", sorted(fnd["finding"].unique().to_list()))
search = st.sidebar.text_input("Name contains")
show_abstain = st.sidebar.checkbox("Show 'insufficient evidence' accounts", value=False)

view = data
if countries:
    view = view.filter(pl.col("country").is_in(countries))
if types:
    view = view.filter(pl.col("finding_types").list.eval(pl.element().is_in(types)).list.any())
if search:
    view = view.filter(pl.col("name").str.to_lowercase().str.contains(search.lower(), literal=True))
if not show_abstain:
    view = view.filter(pl.col("ranked"))
# stable order (join output order is random; ties in score must not swap between reruns,
# or the account dropdown sees new options and resets to the first one)
view = view.sort(["ranked", "score", "owner_key"], descending=[True, True, False])

tabs = st.tabs(["Accounts", "How this works"] + (["Ask"] if ASK["enabled"] else []))
tab_list, tab_about = tabs[0], tabs[1]

# ---------------- ranked list ----------------
with tab_list:
    n_ranked = view["ranked"].sum()
    st.subheader(f"{n_ranked} ranked accounts" + (f" + {view.height - n_ranked} with insufficient evidence"
                                                  if show_abstain else ""))
    table = view.select(
        pl.when(pl.col("ranked")).then(pl.col("score").round(1)).alias("score"),   # no guessed score
        "name", "country", pl.col("domain").alias("domain seen"), pl.col("n_ips").alias("IPs"),
        pl.col("confidence").round(2),
        pl.when(pl.col("ranked")).then(pl.lit("ranked")).otherwise(pl.lit("insufficient evidence")).alias("status"),
        pl.col("reasons").list.first().alias("top reason"), "pitch",
    )
    st.dataframe(table, hide_index=True, width="stretch", height=420, column_config={
        "confidence": st.column_config.ProgressColumn("confidence", min_value=0.0, max_value=1.0, format="%.2f"),
    })
    csv = view.with_columns(pl.col("reasons").list.join(" | "), pl.col("finding_types").list.join(" | ")).drop("owner_key")
    st.download_button("Download CSV", csv.write_csv(), "accounts.csv", "text/csv")

    # ---------------- account detail ----------------
    st.divider()
    if view.height == 0:
        st.info("No accounts match these filters.")
    else:
        names = dict(zip(view["owner_key"], view["name"]))
        pick = st.selectbox("Account detail", list(names), format_func=names.get, key="account")
        a = view.filter(pl.col("owner_key") == pick).row(0, named=True)
        rows = fnd.filter(pl.col("owner_key") == a["owner_key"])

        explain = EXPLAIN["findings"]
        rank = view["owner_key"].to_list().index(pick) + 1
        c1, c2, c3, c4 = st.columns(4)
        c1.metric(f"Rank (of {n_ranked})", f"#{rank}" if a["ranked"] else "none")   # short values: no cut-off
        c2.metric("Risk score", f"{a['score']:.1f}" if a["ranked"] else "n/a")
        c3.metric(f"How sure ({a['confidence']:.2f})", word(EXPLAIN["confidence_words"], a["confidence"]))
        c4.metric("Servers", a["n_ips"])
        if not a["ranked"]:
            st.warning(f"Not enough proof: confidence {a['confidence']:.2f} is below {threshold:.2f}, "
                       "so we do not rank this account or give it a score.")

        # --- what to pitch: one block per problem type, in score order ---
        st.markdown("#### What to pitch")
        st.caption(EXPLAIN["tone"])
        per = {r["finding"]: r for r in rows.group_by("finding").agg(
            pl.col("ip_str").n_unique().alias("n"), pl.col("severity").max().alias("sev"),
            pl.col("pitch").first()).iter_rows(named=True)}
        for i, reason in enumerate(a["reasons"], 1):
            f = per[reason.split(" on ")[0]]
            e = explain.get(f["finding"], {})
            st.markdown(
                f"**{i}. {e.get('title', f['finding'])}**: {word(EXPLAIN['severity_words'], f['sev'])} risk, "
                f"seen on {f['n']} server{'s' if f['n'] > 1 else ''}. Product to offer: *{f['pitch']}*\n"
                f"- **What it means:** {e.get('what', '-')}\n"
                f"- **Why they should care:** {e.get('why', '-')}\n"
                f"- **How to open:** \"{e.get('talk', '-')}\"")

        # --- how sure are we: two plain questions ---
        st.markdown(f"#### How sure are we? {word(EXPLAIN['confidence_words'], a['confidence'])} "
                    f"({a['confidence']:.2f} out of 1)")
        st.markdown(
            f"We answer two questions, each from 0 (no idea) to 1 (certain), and multiply them:\n\n"
            f"1. **Are these really their servers?** {a['owner_proof']:.2f}. Mainly because "
            f"{EXPLAIN['owner_proof'].get(a['main_tier'], '?')}"
            + (", and a second, independent source names the same company." if a["proofs_agree"] else ".")
            + f"\n2. **Are the problems real?** {a['evidence']:.2f}. {EXPLAIN['evidence_note']}\n\n"
            f"{a['owner_proof']:.2f} x {a['evidence']:.2f} = **{a['confidence']:.2f}**. "
            f"Accounts below {threshold:.2f} are not ranked: a wrong lead wastes a rep's time.")

        st.markdown("#### Evidence: every problem we saw (send this to the customer if useful)")
        st.dataframe(rows.sort(["severity", "ts"], descending=True).select(
            pl.col("finding").replace_strict({k: v["title"] for k, v in explain.items()}, default=pl.col("finding"))
              .alias("problem"),
            pl.col("ip_str").alias("server (IP)"), "port", pl.col("severity").round(1).alias("risk 0-10"),
            pl.col("detail").alias("technical detail"), pl.col("ts").dt.date().alias("seen on")),
            hide_index=True, width="stretch")

# ---------------- about ----------------
with tab_about:
    w = SCORING["attribution_weight"]
    st.markdown(f"""
### What this app does
It finds companies that have **visible security problems on their internet-facing servers** and ranks
them, so a sales rep knows who to call first, why, and what to offer.

The data is an internet scan: a robot knocks on every internet address and writes down what answers
(one record per server address and port). It is **not** company data, so we first have to work out
which company owns each server. This demo uses the first 500,000 records of a 13.5-million-record scan file.

### The steps, in plain words
1. **Throw away servers whose owner is not a buyer.** Cloud, hosting, CDN and internet providers own the
   address but rent it to someone else, so a problem there is not *their* problem. Home internet too.
2. **Work out who owns each remaining server**, from three clues:
   - its security certificate names the company (strongest clue)
   - its web addresses all belong to one company domain
   - the internet registry lists the company as owner of the address

   If no clue works, the server is dropped. We prefer "don't know" to a wrong guess.
3. **Spot problems** with fixed, simple rules: known security holes, remote logins or databases open to
   the internet, software past end of life, old encryption, expired certificates.
4. **Clean up names with AI (done once, offline).** An AI model reads each owner name and flags ones that are
   not a real customer, e.g. a hosting company or a device maker whose name is printed on certificates of
   devices it sold. It is checked against a gold set of 110 names. The app itself makes no AI calls.
5. **Rank.** Each problem type adds points: how bad it is (0-10) x how many servers have it (more servers
   add points, but less each time) x how recent it is.

### How sure are we? (confidence)
Two questions, each 0 to 1, multiplied:
- **Are these really their servers?** Certificate clue {w['A_cert']}, domain clue {w['B_hostname']},
  registry clue {w['C_org']}; +{w['agreement']['bonus']} when two clues name the same company (max 1).
- **Are the problems real?** {EXPLAIN['evidence_note']}

Example: certificate + a second clue agree (1.00) x problems read straight from the server (0.90) = 0.90, *High*.
Below the slider value (default {SCORING['abstain_below']}) the app shows "not enough proof" instead of a score.
This number is recalculated live in the app from the evidence rows.

### How well it works (provisional)
Among the top 25 accounts, real companies went from 10 (rules only) to 15 (rules + AI clean-up),
judged on a gold set of 96 names from this data. Known misses: some software makers (Bitnami, Bitrix) still rank.
The gold labels were drafted with AI help and have not yet been reviewed by a person, so treat these numbers as early.
Details: docs/EVALUATION.md.
""")

# ---------------- ask (optional, config/ask.yaml) ----------------
if ASK["enabled"]:
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import ask

    with tabs[2]:
        key = ask.api_key(ASK)
        if not key:
            st.info(f"Ask is on but no API key is set: put {' or '.join(ASK['api_key_env'])} in .env.")
        else:
            @st.cache_data(show_spinner=False)
            def cached_ask(question: str) -> dict:          # same question again = no new API call
                return ask.ask(question, acc, fnd, ASK, ask.make_client(key))

            st.caption("Answers come from an AI model, using only the accounts in this app. "
                       "The rows it used are shown under the answer.")
            question = st.text_input("Ask about the accounts",
                                     placeholder="e.g. Which companies in Brazil have open remote desktop?", key="ask_q")
            if question.strip():
                with st.spinner("Thinking..."):
                    try:
                        res = cached_ask(question.strip())
                    except Exception as e:                # network / rate limit / bad key
                        st.error(f"The AI call failed: {type(e).__name__}: {str(e)[:200]}")
                        res = None
                if res:
                    st.markdown(res["answer"])
                    if res["rows"] is not None:
                        with st.expander(f"Data used: {res['tool']} {res['args']} ({res['rows'].height} rows)"):
                            st.dataframe(res["rows"], hide_index=True, width="stretch")
