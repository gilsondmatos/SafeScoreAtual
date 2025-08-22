"""
Coletor ETH sem dependência do web3.py (usa JSON-RPC direto).
Se falhar, o dashboard cai no mock automaticamente.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional
import time
import requests

def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}

def _rpc_call(url: str, method: str, params: list[Any]) -> Any:
    resp = requests.post(
        url,
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise RuntimeError(data["error"])
    return data["result"]

def _hex_to_int(h: str) -> int:
    return int(h, 16)

def _wei_to_eth(wei_hex: str) -> float:
    try:
        val = int(wei_hex, 16)
        return val / 1e18
    except Exception:
        return 0.0

def _get_urls() -> List[str]:
    urls = os.getenv("ETH_RPC_URL", "") or ""
    urls = urls.replace(" ", "")
    return [u for u in urls.split(",") if u]

def load_from_eth(data_dir: Path) -> List[Dict[str, Any]]:
    urls = _get_urls()
    if not urls:
        raise RuntimeError("ETH_RPC_URL vazio")

    blocks_back = int(os.getenv("ETH_BLOCKS_BACK", "20"))
    max_tx = int(os.getenv("ETH_MAX_TX", "50"))
    only_erc20 = _env_bool("ETH_ONLY_ERC20", False)
    min_eth = float(os.getenv("ETH_INCLUDE_ETH_VALUE_MIN", "0.0"))

    last_block: Optional[int] = None
    used_url: Optional[str] = None
    for u in urls:
        try:
            h = _rpc_call(u, "eth_blockNumber", [])
            last_block = _hex_to_int(h)
            used_url = u
            break
        except Exception:
            continue
    if last_block is None:
        raise RuntimeError("Nenhum RPC respondeu a eth_blockNumber")

    out: List[Dict[str, Any]] = []
    start = max(0, last_block - blocks_back + 1)

    for b in range(last_block, start - 1, -1):
        if len(out) >= max_tx:
            break
        try:
            blk = _rpc_call(used_url, "eth_getBlockByNumber", [hex(b), True])
        except Exception:
            blk = None
            for alt in urls:
                if alt == used_url:
                    continue
                try:
                    blk = _rpc_call(alt, "eth_getBlockByNumber", [hex(b), True])
                    used_url = alt
                    break
                except Exception:
                    pass
            if blk is None:
                continue

        ts = _hex_to_int(blk.get("timestamp", "0x0"))
        txs = blk.get("transactions", []) or []

        for t in txs:
            if len(out) >= max_tx:
                break
            value_eth = _wei_to_eth(t.get("value", "0x0"))
            if value_eth < min_eth:
                continue
            if only_erc20:
                continue

            tx_id = t.get("hash", "")
            from_addr = t.get("from", "")
            to_addr = t.get("to", "") or ""
            method = "TRANSFER" if (t.get("input", "0x") == "0x") else "CALL"

            out.append({
                "tx_id": tx_id,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(ts)),
                "from_address": from_addr,
                "to_address": to_addr,
                "amount": round(value_eth, 8),
                "token": "ETH",
                "method": method,
                "chain": os.getenv("CHAIN_NAME", "ETH"),
            })

    return out
