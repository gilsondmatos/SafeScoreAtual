import os
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any

# Carregar .env se existir
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

from app.engine.scoring import ScoreEngine
from app.alerts.telegram import TelegramAlerter
from app.collectors.mock_collector import load_input_or_mock

def try_load_eth_collector():
    try:
        from app.collectors.eth_collector import load_from_eth  # type: ignore
        return load_from_eth
    except Exception:
        return None

DATA_DIR = Path("app/data")
TX_CSV_GLOBAL = DATA_DIR / "transactions.csv"
KNOWN_CSV = DATA_DIR / "known_addresses.csv"
PENDING_CSV = DATA_DIR / "pending_review.csv"

# -------- util --------
def safe_text(text: str) -> str:
    if text is None:
        return ""
    t = str(text)
    t = (
        t.replace("—", "-").replace("–", "-").replace("…", "...")
         .replace("≥", ">=").replace("≤", "<=")
         .replace("•", "-")
    )
    return t.encode("latin-1", "replace").decode("latin-1")

def ensure_data_files():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not KNOWN_CSV.exists():
        with KNOWN_CSV.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["address","first_seen"])
            writer.writeheader()

def read_known_addresses() -> set:
    if not KNOWN_CSV.exists():
        return set()
    with KNOWN_CSV.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return {row["address"].strip() for row in reader if row.get("address")}

def append_known_address(addr: str):
    addr = (addr or "").strip()
    if not addr:
        return
    known = read_known_addresses()
    if addr in known:
        return
    with KNOWN_CSV.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["address","first_seen"])
        writer.writerow({"address": addr, "first_seen": datetime.now(timezone.utc).isoformat()})

def read_prev_transactions(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)

def _write_rows_to(path: Path, rows: List[Dict[str, Any]], header: list[str]):
    file_exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        if not file_exists:
            writer.writeheader()
        for r in rows:
            writer.writerow(r)

def write_transactions_global(rows: List[Dict[str, Any]]):
    header = [
        "tx_id","timestamp","from_address","to_address","amount","token","method","chain",
        "is_new_address","velocity_last_window","score","penalty_total","reasons","explain"
    ]
    _write_rows_to(TX_CSV_GLOBAL, rows, header)
    day_file = DATA_DIR / f"transactions_{datetime.now().strftime('%Y%m%d')}.csv"
    _write_rows_to(day_file, rows, header)

def write_transactions_by_chain(rows: List[Dict[str, Any]], chain_label: str):
    header = [
        "tx_id","timestamp","from_address","to_address","amount","token","method","chain",
        "is_new_address","velocity_last_window","score","penalty_total","reasons","explain"
    ]
    suf = chain_label.lower()
    chain_file = DATA_DIR / f"transactions_{suf}.csv"
    chain_day = DATA_DIR / f"transactions_{suf}_{datetime.now().strftime('%Y%m%d')}.csv"
    _write_rows_to(chain_file, rows, header)
    _write_rows_to(chain_day, rows, header)
    return chain_file, chain_day

def clear_tx_files(chain_label: str):
    """Se CLEAR_TX_ON_RUN=true, remove CSVs agregados e por-chain antes de escrever."""
    flag = str(os.getenv("CLEAR_TX_ON_RUN", "false")).lower() in ("1","true","yes","y")
    if not flag:
        return
    date_tag = datetime.now().strftime('%Y%m%d')
    try:
        # globais
        if TX_CSV_GLOBAL.exists():
            TX_CSV_GLOBAL.unlink()
        day_global = DATA_DIR / f"transactions_{date_tag}.csv"
        if day_global.exists():
            day_global.unlink()
        # por chain
        suf = (chain_label or "unknown").lower()
        chain_file = DATA_DIR / f"transactions_{suf}.csv"
        chain_day = DATA_DIR / f"transactions_{suf}_{date_tag}.csv"
        if chain_file.exists():
            chain_file.unlink()
        if chain_day.exists():
            chain_day.unlink()
        print(f"[CLEAN] Limpei CSVs anteriores (global e por chain='{suf}').")
    except Exception as e:
        print(f"[WARN] Falha ao limpar CSVs: {e}")

def append_pending(rows: List[Dict[str, Any]]):
    header = [
        "tx_id","timestamp","from_address","to_address","amount","token","method","chain",
        "score","penalty_total","reasons","explain"
    ]
    _write_rows_to(PENDING_CSV, [
        {
            "tx_id": r["tx_id"],
            "timestamp": r["timestamp"],
            "from_address": r["from_address"],
            "to_address": r["to_address"],
            "amount": r["amount"],
            "token": r["token"],
            "method": r["method"],
            "chain": r["chain"],
            "score": r["score"],
            "penalty_total": r.get("penalty_total", 0),
            "reasons": r["reasons"],
            "explain": r.get("explain","{}"),
        } for r in rows
    ], header)

def abbreviate(addr: str) -> str:
    if not addr:
        return ""
    return addr if len(addr) <= 10 else f"{addr[:6]}…{addr[-4:]}"

def pick_collector() -> str:
    for i, a in enumerate(sys.argv):
        if a in ("--collector", "-c") and i + 1 < len(sys.argv):
            return sys.argv[i + 1].strip().lower()
        if a.startswith("--collector="):
            return a.split("=", 1)[1].strip().lower()
    return os.getenv("COLLECTOR", "mock").strip().lower()

def main():
    ensure_data_files()

    try:
        threshold = int(os.getenv("SCORE_ALERT_THRESHOLD", "50"))
    except Exception:
        threshold = 50

    collector = pick_collector()
    chain_label = os.getenv("CHAIN_NAME", "ETH") if collector == "eth" else "MOCK"

    # limpeza opcional de CSVs para garantir "só Ethereum" quando rodar com coletor eth
    clear_tx_files(chain_label)

    # histórico para regra de velocidade vem do CSV GLOBAL (se existir)
    prev_global = read_prev_transactions(TX_CSV_GLOBAL)
    known = read_known_addresses()

    engine = ScoreEngine(data_dir=str(DATA_DIR), prev_transactions=prev_global, known_addresses=known)
    alerter = TelegramAlerter.from_env()

    # ---- seleção de coletor ----
    txs: List[Dict[str, Any]] = []
    if collector == "eth":
        load_from_eth = try_load_eth_collector()
        if load_from_eth is None:
            print("[WARN] Coletor ETH indisponível. Usando mock.")
            txs = load_input_or_mock(DATA_DIR)
            chain_label = "MOCK"
        else:
            txs = load_from_eth(DATA_DIR)
            if not txs:
                print("[WARN] Coletor ETH não retornou dados. Usando mock.")
                txs = load_input_or_mock(DATA_DIR)
                chain_label = "MOCK"
    else:
        txs = load_input_or_mock(DATA_DIR)
        chain_label = "MOCK"

    out_rows = []
    pendings = []

    for tx in txs:
        scored = engine.score_transaction(tx)
        hits: Dict[str, int] = scored["hits"] or {}
        penalty_total = int(sum(hits.values()))
        contrib_pct = {k: round((v / penalty_total) * 100, 1) for k, v in hits.items()} if penalty_total > 0 else {}
        explain_payload = {"weights": hits, "contrib_pct": contrib_pct}

        reasons_txt = safe_text("; ".join(scored["reasons"]) if scored["reasons"] else "")
        row = {
            "tx_id": tx.get("tx_id",""),
            "timestamp": tx.get("timestamp",""),
            "from_address": tx.get("from_address",""),
            "to_address": tx.get("to_address",""),
            "amount": tx.get("amount",0),
            "token": tx.get("token",""),
            "method": tx.get("method",""),
            "chain": chain_label,
            "is_new_address": "yes" if hits.get("new_address") else "no",
            "velocity_last_window": scored.get("velocity_last_window", 0),
            "score": scored["score"],
            "penalty_total": penalty_total,
            "reasons": reasons_txt,
            "explain": json.dumps(explain_payload, ensure_ascii=False),
        }
        out_rows.append(row)

        if hits.get("new_address"):
            append_known_address(tx.get("from_address",""))

        if row["score"] < threshold:
            pendings.append(row)
            msg = (
                f"🚨 SafeScore ALERTA\n"
                f"TX: {row['tx_id']}\n"
                f"Score: {row['score']} (< {threshold})\n"
                f"De: {abbreviate(row['from_address'])}\n"
                f"Para: {abbreviate(row['to_address'])}\n"
                f"Valor: {row['amount']} {row['token']}\n"
                f"Motivos: {row['reasons'] or 'n/d'}"
            )
            alerter.send(msg)

    if out_rows:
        write_transactions_global(out_rows)             # mantém consolidado
        chain_file, chain_day = write_transactions_by_chain(out_rows, chain_label)
    else:
        chain_file = DATA_DIR / f"transactions_{chain_label.lower()}.csv"
        chain_day  = DATA_DIR / f"transactions_{chain_label.lower()}_{datetime.now().strftime('%Y%m%d')}.csv"

    if pendings:
        append_pending(pendings)

    print(f"[OK] Processadas {len(txs)} transações via coletor '{collector}'.")
    print(f"[OUT] Global: {TX_CSV_GLOBAL.name} | Por chain: {chain_file.name} e {chain_day.name}")
    if pendings:
        print(f"[HOLD] {len(pendings)} transações adicionadas a {PENDING_CSV.name} (score < {threshold}).")
    else:
        print("[HOLD] Nenhuma transação crítica para retenção.")

if __name__ == "__main__":
    main()
