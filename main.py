"""
Pipeline principal SafeScore: coletar → pontuar → salvar CSV → alertar
"""
import os
import pandas as pd
from pathlib import Path
from datetime import datetime

from app.collectors.eth_collector import load_from_eth
from app.collectors.mock_collector import load_input_or_mock
from app.scoring import ScoreEngine
from app.alerts.telegram_alert import send_alert

DATA_DIR = Path("app/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

def run_pipeline() -> Path:
    try:
        print("[INFO] Coletando da Ethereum...")
        txs = load_from_eth(DATA_DIR)
        if not txs:
            raise RuntimeError("Sem dados retornados da ETH")
        chain = "ETH"
    except Exception as e:
        print(f"[WARN] Falha na ETH ({e}), usando MOCK.")
        txs = load_input_or_mock(DATA_DIR)
        chain = "MOCK"

    engine = ScoreEngine(data_dir=str(DATA_DIR), prev_transactions=[], known_addresses=set())
    rows = []
    for tx in txs:
        scored = engine.score_transaction(tx)
        hits = scored.get("hits", {}) or {}
        rows.append({
            **tx,
            "is_new_address": "yes" if hits.get("new_address") else "no",
            "velocity_last_window": scored.get("velocity_last_window", 0),
            "score": scored["score"],
            "penalty_total": int(sum(hits.values())),
            "reasons": "; ".join(scored.get("reasons", [])),
        })

    today = datetime.now().strftime("%Y%m%d")
    f_daily = DATA_DIR / f"transactions_{chain.lower()}_{today}.csv"
    pd.DataFrame(rows).to_csv(f_daily, index=False)
    print(f"[OK] CSV salvo em {f_daily}")

    threshold = int(os.getenv("SCORE_ALERT_THRESHOLD", "50"))
    critical = [r for r in rows if r.get("score", 100) < threshold]
    if critical:
        send_alert(f"{len(critical)} transações críticas detectadas! Score < {threshold}")
    return f_daily

if __name__ == "__main__":
    run_pipeline()
