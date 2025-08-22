"""
Pipeline principal SafeScore
Executa: coletar → pontuar → salvar CSV → alertar
"""
import os
import pandas as pd
from pathlib import Path
from datetime import datetime

from eth_collector import load_from_eth
from mock_collector import load_input_or_mock
from app.engine.scoring import ScoreEngine
from app.alerts.telegram_alert import send_alert


DATA_DIR = Path("app/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)


def run_pipeline():
    # 1. Coleta (tenta ETH → fallback para MOCK)
    try:
        print("[INFO] Coletando da Ethereum...")
        txs = load_from_eth(DATA_DIR)
        if not txs:
            raise RuntimeError("Sem dados da ETH")
        chain = "ETH"
    except Exception as e:
        print(f"[WARN] Falha na ETH ({e}), usando MOCK.")
        txs = load_input_or_mock(DATA_DIR)
        chain = "MOCK"

    # 2. Scoring
    engine = ScoreEngine(data_dir=str(DATA_DIR), prev_transactions=[], known_addresses=set())
    scored = [engine.score_transaction(tx) for tx in txs]

    # 3. Salvar CSV
    today = datetime.now().strftime("%Y%m%d")
    fname = f"transactions_{chain.lower()}_{today}.csv"
    fpath = DATA_DIR / fname
    pd.DataFrame(scored).to_csv(fpath, index=False)
    print(f"[OK] CSV salvo em {fpath}")

    # 4. Alertas
    threshold = int(os.getenv("SCORE_ALERT_THRESHOLD", "50"))
    critical = [tx for tx in scored if tx.get("score", 100) < threshold]
    if critical:
        send_alert(f"{len(critical)} transações críticas detectadas! Score < {threshold}")

    return fpath


if __name__ == "__main__":
    run_pipeline()
