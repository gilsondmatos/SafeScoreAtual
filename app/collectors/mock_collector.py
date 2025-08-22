from __future__ import annotations
import csv
import random
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Dict, Any

TOKENS = ["ETH","USDT","USDC","DAI"]
METHODS = ["TRANSFER","SWAP","APPROVE"]

def _rand_address() -> str:
    return "0x" + "".join(random.choices("abcdef0123456789", k=40))

def _now_iso_utc_offset(minutes_offset: int = 0) -> str:
    dt = datetime.now(timezone.utc) + timedelta(minutes=minutes_offset)
    return dt.isoformat()

def generate_mock(n: int = 12) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for i in range(n):
        amt = round(random.uniform(5, 25000), 2)
        token = random.choice(TOKENS)
        method = random.choice(METHODS)
        out.append({
            "tx_id": f"MOCK-{int(datetime.now().timestamp())}-{i}",
            "timestamp": _now_iso_utc_offset(-i),  # últimas N minutos
            "from_address": _rand_address(),
            "to_address": _rand_address(),
            "amount": amt,
            "token": token,
            "method": method,
            "chain": "MOCK",
        })
    return out

def load_input_or_mock(data_dir: Path) -> List[Dict[str, Any]]:
    """
    Se existir app/data/input_transactions.csv, carrega de lá.
    Caso contrário, gera dados mock.
    """
    p = data_dir / "input_transactions.csv"
    if not p.exists():
        return generate_mock(12)

    txs: List[Dict[str, Any]] = []
    with p.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            txs.append({
                "tx_id": r.get("tx_id",""),
                "timestamp": r.get("timestamp",""),
                "from_address": r.get("from_address",""),
                "to_address": r.get("to_address",""),
                "amount": float(r.get("amount","0") or 0),
                "token": r.get("token",""),
                "method": r.get("method",""),
                "chain": r.get("chain","MOCK"),
            })
    return txs
