"""classify.py guardrails with a FAKE client (no network, no key)."""
import json
import sys
from pathlib import Path
from types import SimpleNamespace as NS

import polars as pl

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "pipeline"))
from classify import classify, log_cost, parse  # noqa: E402
from common import load_config  # noqa: E402

LABELS = ["company", "infrastructure", "default", "unknown"]


class FakeClient:
    """Answers with a fixed label per name; counts calls; can return junk."""
    def __init__(self, answers: dict, junk: bool = False):
        self.answers, self.junk, self.calls = answers, junk, 0
        self.chat = NS(completions=NS(create=self.create))

    def create(self, model, temperature, response_format, messages):
        self.calls += 1
        assert temperature == 0
        items = json.loads(messages[1]["content"])["names"]
        body = "not json" if self.junk else json.dumps(
            {"results": [{"id": i["id"], "label": self.answers[i["name"]], "reason": "r"}
                         for i in items if i["name"] in self.answers]})
        return NS(choices=[NS(message=NS(content=body))], usage=NS(prompt_tokens=100, completion_tokens=20))


def names(*ns):
    return pl.DataFrame({"name": list(ns), "source": ["cert"] * len(ns)})


def test_parse_keeps_only_valid():
    text = json.dumps({"results": [{"id": 0, "label": "company"}, {"id": 1, "label": "bogus"},
                                   {"id": 9, "label": "default"}, {"id": "2", "label": "default"}]})
    assert parse(text, 3, LABELS) == {0: {"label": "company", "reason": ""}}
    assert parse("not json", 3, LABELS) == {}
    assert parse('{"results": 5}', 3, LABELS) == {}


def test_cache_means_second_run_is_free(tmp_path):
    cfg = load_config("llm")
    cache = tmp_path / "cache.jsonl"
    c = FakeClient({"Ford Motor Company": "company", "Bitnami": "default"})
    df, st = classify(names("Ford Motor Company", "Bitnami"), cfg, c, cache)
    assert dict(zip(df["name"], df["label"])) == {"Ford Motor Company": "company", "Bitnami": "default"}
    assert c.calls == 1 and st["cache_hits"] == 0
    df2, st2 = classify(names("Ford Motor Company", "Bitnami"), cfg, c, cache)
    assert c.calls == 1 and st2["cache_hits"] == 2          # no new API call
    assert df2.equals(df)


def test_missing_or_junk_answer_is_unknown_and_not_cached(tmp_path):
    cfg = {**load_config("llm"), "max_retries": 0}
    cache = tmp_path / "cache.jsonl"
    df, _ = classify(names("Acme", "Zzz"), cfg, FakeClient({"Acme": "company"}), cache)
    assert dict(zip(df["name"], df["label"])) == {"Acme": "company", "Zzz": "unknown"}
    df, _ = classify(names("Other"), cfg, FakeClient({}, junk=True), cache)
    assert df["label"].to_list() == ["unknown"]
    cached = [json.loads(line)["name"] for line in open(cache)]
    assert cached == ["Acme"]                                 # Zzz / Other retry next run


def test_load_env(tmp_path, monkeypatch):
    from common import load_env
    env = tmp_path / ".env"
    env.write_text('# comment\nT_NEW_KEY="abc"\nT_SET_KEY=from_file\n\nnot a line\n')
    monkeypatch.delenv("T_NEW_KEY", raising=False)
    monkeypatch.setenv("T_SET_KEY", "from_shell")
    load_env(env)
    import os
    assert os.environ["T_NEW_KEY"] == "abc"           # quotes stripped
    assert os.environ["T_SET_KEY"] == "from_shell"    # shell export wins
    monkeypatch.delenv("T_NEW_KEY")
    load_env(tmp_path / "missing.env")                # no file: no error


def test_cost_logged(tmp_path):
    cfg = load_config("llm")
    usd = log_cost(cfg, {"names": 2, "cache_hits": 0, "calls": 1, "tok_in": 1_000_000, "tok_out": 0},
                   tmp_path / "cost.csv")
    assert usd == cfg["price_usd_per_mtok"]["input"]
    assert (tmp_path / "cost.csv").read_text().count("\n") == 2   # header + 1 run
