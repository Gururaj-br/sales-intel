"""app/ask.py with a fake LLM client: tool choice is checked, tools return the right rows, junk -> "none"."""
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "app"))
import ask  # noqa: E402

ACC = pl.DataFrame({
    "owner_key": ["acme", "bright", "nordic", "ghost"],
    "name": ["Acme Retail", "Bright Dental", "Nordic Foods", "Ghost Org"],
    "main_tier": ["A", "B", "A", "C"],
    "country": ["BR", "US", "BR", "BR"],
    "domain": ["acme.com.br", "brightdental.com", None, None],
    "score": [24.8, 9.0, 8.0, 12.0],
    "confidence": [0.8, 0.64, 0.72, 0.45],
    "reasons": [["has_cve on 1 IP (sev 9.8)"], ["rdp on 1 IP (sev 9.0)"], ["rdp on 2 IPs (sev 9.0)"], ["rdp"]],
    "pitch": ["Vulnerability management", "Zero trust access", "Zero trust access", "Zero trust access"],
    "status": ["ranked", "ranked", "ranked", "abstain"],
})
FND = pl.DataFrame({"owner_key": ["acme", "bright", "nordic", "nordic", "ghost"],
                    "finding": ["has_cve", "rdp", "rdp", "rdp", "rdp"]})
FINDINGS = ["has_cve", "rdp"]


class FakeClient:
    """Answers the tool call with `tool_json`, then echoes how many rows it was given."""
    def __init__(self, tool_json: str):
        self.tool_json, self.calls = tool_json, []
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self.create))

    def create(self, model, temperature, messages, **kw):
        self.calls.append(messages[1]["content"])
        text = self.tool_json if "response_format" in kw else f"answer from {messages[1]['content'].count('name')} rows"
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=text))],
                               usage=SimpleNamespace(prompt_tokens=100, completion_tokens=20))


@pytest.fixture
def cfg(tmp_path):
    c = yaml.safe_load((ROOT / "config/ask.yaml").read_text())
    c["cost_log"] = str(tmp_path / "ask_cost.csv")
    return c


def test_config_is_safe_for_a_public_link():
    c = yaml.safe_load((ROOT / "config/ask.yaml").read_text())
    assert 0 < c["max_questions_per_session"] <= 20                   # caps what one visitor can spend
    assert all(k.isupper() and not k.startswith("gsk_") for k in c["api_key_env"])   # env var NAMES, never a key
    assert "groq" in (ROOT / "app/requirements.txt").read_text()       # hosted app can import the SDK


def test_top_accounts_only_ranked_and_filters():
    rows = ask.top_accounts(ACC, FND, 25, country="br", finding="rdp")
    assert rows["name"].to_list() == ["Nordic Foods"]            # Ghost Org abstains, Acme has no rdp
    assert ask.top_accounts(ACC, FND, 25)["name"].to_list() == ["Acme Retail", "Bright Dental", "Nordic Foods"]
    assert ask.top_accounts(ACC, FND, 2, n=50).height == 2       # capped by max_rows


def test_find_account_shows_abstained_with_status():
    rows = ask.find_account(ACC, FND, 25, name="ghost")
    assert rows["status"].to_list() == ["abstain"]
    assert ask.find_account(ACC, FND, 25, name="brightdental.com")["name"].to_list() == ["Bright Dental"]
    assert ask.find_account(ACC, FND, 25, name="").height == 0


def test_count_by():
    assert ask.count_by(ACC, FND, 25, field="country").rows() == [("BR", 2), ("US", 1)]
    assert ask.count_by(ACC, FND, 25, field="finding").rows() == [("rdp", 2), ("has_cve", 1)]   # per account


@pytest.mark.parametrize("text", [
    "not json", '{"tool": "drop_table"}', '{"tool": "top_accounts", "args": {"finding": "made_up"}}',
    '{"tool": "count_by", "args": {"field": "ip_str"}}', '{"tool": "top_accounts", "args": {"n": "lots"}}',
    '{"tool": "none", "args": {}}',
])
def test_bad_tool_answers_become_none(text):
    assert ask.pick_tool(text, FINDINGS) == ("none", {})


def test_unknown_args_are_dropped():
    assert ask.pick_tool('{"tool": "top_accounts", "args": {"country": "BR", "sql": "x", "n": "5"}}', FINDINGS) \
        == ("top_accounts", {"country": "BR", "n": 5})


def test_ask_runs_tool_then_answers_from_rows(cfg):
    client = FakeClient(json.dumps({"tool": "top_accounts", "args": {"country": "BR"}}))
    res = ask.ask("top companies in Brazil?", ACC, FND, cfg, client)
    assert res["tool"] == "top_accounts" and res["rows"]["name"].to_list() == ["Acme Retail", "Nordic Foods"]
    assert "Acme Retail" in client.calls[1] and "Ghost Org" not in client.calls[1]   # model sees only these rows
    assert res["tok_in"] == 200
    assert Path(cfg["cost_log"]).read_text().count("\n") == 2                      # header + this question


def test_ask_none_makes_one_call_only(cfg):
    client = FakeClient('{"tool": "none", "args": {}}')
    res = ask.ask("what's the weather?", ACC, FND, cfg, client)
    assert res["answer"] == ask.NO_TOOL and res["rows"] is None and len(client.calls) == 1
