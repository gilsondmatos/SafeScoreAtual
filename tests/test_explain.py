import sys
from pathlib import Path
import json
import math

# Adiciona raiz do repo ao path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.engine.scoring import ScoreEngine  # type: ignore

def make_data_dir(tmp_path: Path) -> Path:
    data = tmp_path / "app" / "data"
    data.mkdir(parents=True, exist_ok=True)
    # listas mínimas
    (data / "blacklist.csv").write_text("address\n", encoding="utf-8")
    (data / "watchlist.csv").write_text("address\n", encoding="utf-8")
    (data / "sensitive_tokens.csv").write_text("value\nUSDT\n", encoding="utf-8")
    (data / "sensitive_methods.csv").write_text("value\nAPPROVE\n", encoding="utf-8")
    (data / "known_addresses.csv").write_text("address,first_seen\n", encoding="utf-8")
    return data

def test_score_and_explain_sums_to_100(tmp_path):
    data_dir = make_data_dir(tmp_path)
    engine = ScoreEngine(data_dir=str(data_dir), prev_transactions=[], known_addresses=set())

    # Tx que dispara várias regras: valor alto, método sensível, token sensível e horário incomum (03:00Z)
    tx = {
        "tx_id": "T1",
        "timestamp": "2025-01-01T03:00:00+00:00",
        "from_address": "0xabc",
        "to_address": "0xdef",
        "amount": 20000,
        "token": "USDT",
        "method": "APPROVE",
        "chain": "MOCK",
    }
    scored = engine.score_transaction(tx)
    hits = scored["hits"]
    penalty_total = sum(hits.values())
    assert penalty_total > 0
    # contrib como em main.py
    contrib_pct = {k: round((v / penalty_total) * 100, 1) for k, v in hits.items()}
    total_pct = sum(contrib_pct.values())
    assert math.isclose(total_pct, 100.0, abs_tol=0.2)
    # score = 100 - sum(penalidades) limitado a [0,100]
    expected = max(0, 100 - penalty_total)
    assert scored["score"] == expected

def test_score_upper_bound(tmp_path):
    data_dir = make_data_dir(tmp_path)
    # Marcar o remetente como "conhecido" para não acionar a regra new_address
    known = {"0xknown"}
    engine = ScoreEngine(data_dir=str(data_dir), prev_transactions=[], known_addresses=known)

    # nenhuma regra aciona -> score 100
    tx = {
        "tx_id": "T2",
        "timestamp": "2025-01-01T12:00:00+00:00",
        "from_address": "0xknown",
        "to_address": "0xzzz",
        "amount": 10,
        "token": "DAI",
        "method": "TRANSFER",
        "chain": "MOCK",
    }
    scored = engine.score_transaction(tx)
    assert scored["score"] == 100
