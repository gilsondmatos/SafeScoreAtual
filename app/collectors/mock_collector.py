"""
Gerador mock *sem* Streamlit, para demo/offline.
"""
from __future__ import annotations

import random
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List


TOKENS = ["ETH", "USDT", "USDC", "DAI"]
METHODS = ["TRANSFER", "SWAP", "APPROVE"]
CHAINS = ["MOCK"]  # explicita


def _rand_addr() -> str:
    return "0x" + "".join(random.choices("0123456789abcdef", k=40))


def load_input_or_mock(data_dir: Path) -> List[Dict[str, Any]]:
    random.seed(datetime.now(timezone.utc).strftime("%Y%m%d%H"))  # muda por hora
    now = datetime.now(timezone.utc)

    out: List[Dict[str, Any]] = []
    for i in range(50):
        ts = now - timedelta(minutes=random.randint(0, 600))
        token = random.choice(TOKENS)
        method = random.choice(METHODS)
        amt = round(random.uniform(1, 25000), 2)
        out.append({
            "tx_id": f"MOCK-{int(now.timestamp())}-{i}",
            "timestamp": ts.isoformat(),
            "from_address": _rand_addr(),
            "to_address": _rand_addr(),
            "amount": amt,
            "token": token,
            "method": method,
            "chain": "MOCK",
        })
    return out
