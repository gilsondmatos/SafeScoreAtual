from __future__ import annotations
import os
import json
import requests
from requests.adapters import HTTPAdapter, Retry
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

# Seletores de funções (ERC-20)
SIG_SYMBOL   = "0x95d89b41"
SIG_DECIMALS = "0x313ce567"
SIG_TRANSFER = "0xa9059cbb"  # transfer(address,uint256)
SIG_APPROVE  = "0x095ea7b3"  # approve(address,uint256)

# ---------- HTTP helpers ----------
def _make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=3, backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST"]
    )
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "Content-Type": "application/json",
        "User-Agent": "SafeScore/1.0 (+demo)"
    })
    return s

def _rpc_any(urls: List[str], method: str, params: list[Any], timeout: float = 10.0, return_url: bool = False) -> Any:
    """Tenta RPC em ordem nos endpoints; retorna o primeiro resultado bem sucedido.
       Se return_url=True, retorna (result, url_escolhida)."""
    sess = _make_session()
    body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    last_err: Optional[str] = None
    for url in urls:
        try:
            r = sess.post(url, json=body, timeout=timeout)
            r.raise_for_status()
            data = r.json()
            if "error" in data:
                last_err = f"{data['error']}"
                continue
            return (data["result"], url) if return_url else data["result"]
        except Exception as e:
            last_err = str(e)
            continue
    raise RuntimeError(last_err or "RPC failed")

# ---------- decoders / utils ----------
def _hex_to_int(h: Any) -> int:
    if isinstance(h, int):
        return h
    if not h:
        return 0
    return int(str(h), 16) if str(h).startswith("0x") else int(str(h))

def _to_hex(n: int) -> str:
    return hex(int(n))

def _parse_address32(hex64: str) -> str:
    """Extrai address (últimos 20 bytes) de 32 bytes hex (sem 0x, 64 chars)."""
    if not hex64 or len(hex64) != 64:
        return ""
    return "0x" + hex64[-40:]

def _decode_symbol(raw_hex: str) -> str:
    if not raw_hex or raw_hex == "0x":
        return "TOKEN"
    h = raw_hex[2:] if raw_hex.startswith("0x") else raw_hex
    try:
        # dinâmica: [offset][len][bytes...]
        if len(h) >= 64 * 3:
            strlen = int(h[64:128], 16)
            sbytes = bytes.fromhex(h[128:128 + strlen * 2])
        else:
            sbytes = bytes.fromhex(h).rstrip(b"\x00")
        s = sbytes.decode("utf-8", errors="ignore").strip()
        return (s or "TOKEN")[:12]
    except Exception:
        return "TOKEN"

def _decode_decimals(raw_hex: str) -> int:
    if not raw_hex or raw_hex == "0x":
        return 18
    try:
        h = raw_hex[2:] if raw_hex.startswith("0x") else raw_hex
        if len(h) >= 64:
            return int(h[-64:], 16)
        return int(h, 16)
    except Exception:
        return 18

def _parse_list_env(key: str) -> List[str]:
    raw = os.getenv(key, "")
    items = [x.strip() for x in raw.split(",") if x.strip()]
    return items

def _passes_filters(tx: Dict[str, Any], allow_from: List[str], allow_to: List[str]) -> bool:
    if not allow_from and not allow_to:
        return True
    f = (tx.get("from") or "").lower()
    t = (tx.get("to") or "").lower()
    ok_from = (not allow_from) or (f in allow_from)
    ok_to   = (not allow_to) or (t in allow_to)
    return ok_from and ok_to

# ---------- cache ERC-20 ----------
def _load_cache(data_dir: Path) -> Dict[str, Dict[str, Any]]:
    fp = data_dir / "token_cache.json"
    if not fp.exists():
        return {}
    try:
        return json.loads(fp.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}

def _save_cache(data_dir: Path, cache: Dict[str, Dict[str, Any]]) -> None:
    try:
        (data_dir / "token_cache.json").write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass

def _erc20_meta(rpc_urls: List[str], token_addr: str, cache: Dict[str, Dict[str, Any]]) -> Tuple[str, int]:
    token_addr_lc = (token_addr or "").lower()
    if token_addr_lc in cache:
        meta = cache[token_addr_lc]
        return str(meta.get("symbol") or "TOKEN")[:12], int(meta.get("decimals") or 18)

    symbol_hex = ""
    decimals_hex = ""
    try:
        symbol_hex = _rpc_any(rpc_urls, "eth_call", [{"to": token_addr, "data": "0x95d89b41"}, "latest"])
    except Exception:
        pass
    try:
        decimals_hex = _rpc_any(rpc_urls, "eth_call", [{"to": token_addr, "data": "0x313ce567"}, "latest"])
    except Exception:
        pass
    symbol = _decode_symbol(symbol_hex) if symbol_hex else "TOKEN"
    decimals = _decode_decimals(decimals_hex) if decimals_hex else 18
    if not (0 <= decimals <= 36):
        decimals = 18

    cache[token_addr_lc] = {"symbol": symbol, "decimals": int(decimals)}
    return symbol, int(decimals)

def _decode_tx(rpc_urls: List[str], tx: Dict[str, Any], block_ts_iso: str, cache: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    tx_hash = tx.get("hash", "")
    from_addr = tx.get("from", "")
    to_addr = tx.get("to", "") or ""
    input_data = tx.get("input", "0x") or "0x"
    value_wei = _hex_to_int(tx.get("value", "0x0"))

    # Defaults (transfer de ETH)
    token = "ETH"
    method = "TRANSFER" if value_wei > 0 and input_data in ("0x", "0x0") else "CALL"
    amount = value_wei / 1e18
    to_for_output = to_addr

    if input_data and input_data != "0x":
        selector = input_data[:10].lower()
        params = input_data[10:]
        p1 = params[:64] if len(params) >= 64 else ""
        p2 = params[64:128] if len(params) >= 128 else ""

        # ERC-20 transfer
        if selector == SIG_TRANSFER and p1 and p2:
            to_for_output = _parse_address32(p1)
            raw = int(p2, 16)
            sym, dec = _erc20_meta(rpc_urls, to_addr, cache)
            token = sym
            method = "TRANSFER"
            try:
                amount = raw / (10 ** dec)
            except Exception:
                amount = float(raw)
        # ERC-20 approve
        elif selector == SIG_APPROVE and p1 and p2:
            to_for_output = _parse_address32(p1)  # spender
            raw = int(p2, 16)
            sym, dec = _erc20_meta(rpc_urls, to_addr, cache)
            token = sym
            method = "APPROVE"
            try:
                amount = raw / (10 ** dec)
            except Exception:
                amount = float(raw)

    return {
        "tx_id": tx_hash,
        "timestamp": block_ts_iso,
        "from_address": from_addr,
        "to_address": to_for_output,
        "amount": amount,
        "token": token,
        "method": method,
        "chain": "ETH",
    }

def load_from_eth(data_dir: Path) -> List[Dict[str, Any]]:
    """
    Coletor on-chain via JSON-RPC (requests) com:
    - Fallback de endpoints (ETH_RPC_URL pode ser lista separada por vírgula)
    - Cache de metadados ERC-20 em app/data/token_cache.json
    - Telemetry: salva app/data/last_eth_meta.json com resumo da coleta
    - Filtros:
        * ETH_ONLY_ERC20=true|false
        * ETH_INCLUDE_ETH_VALUE_MIN=0.0 (valor mínimo em ETH p/ incluir transfers nativos)
        * ETH_FILTER_FROM/ETH_FILTER_TO (lista de endereços, vírgula)
    """
    urls = _parse_list_env("ETH_RPC_URL") or [
        "https://ethereum.publicnode.com",
        "https://eth.llamarpc.com",
        "https://cloudflare-eth.com",
    ]
    blocks_back = int(os.getenv("ETH_BLOCKS_BACK", "20"))
    max_tx = int(os.getenv("ETH_MAX_TX", "100"))
    allow_from = [x.lower() for x in _parse_list_env("ETH_FILTER_FROM")]
    allow_to   = [x.lower() for x in _parse_list_env("ETH_FILTER_TO")]
    only_erc20 = str(os.getenv("ETH_ONLY_ERC20", "false")).lower() in ("1", "true", "yes", "y")
    try:
        eth_min = float(os.getenv("ETH_INCLUDE_ETH_VALUE_MIN", "0"))
    except Exception:
        eth_min = 0.0
    eth_min_wei = int(eth_min * 1e18)

    meta_path = data_dir / "last_eth_meta.json"
    meta: Dict[str, Any] = {
        "rpc_urls": urls, "selected_url": None, "chain_id": None,
        "blocks_back": blocks_back, "max_tx": max_tx,
        "filters": {
            "allow_from": allow_from, "allow_to": allow_to,
            "only_erc20": only_erc20, "eth_min": eth_min,
        },
        "latest_block": None, "start_block": None, "collected": 0
    }

    # Health-check (chainId) só para validar rapidamente os endpoints
    try:
        chain_id, used_url = _rpc_any(urls, "eth_chainId", [], return_url=True)
        meta["chain_id"] = chain_id
        meta["selected_url"] = used_url
    except Exception as e:
        print(f"[WARN] Falha no ETH RPC (chainId): {e}")
        meta["error"] = str(e)
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        return []

    try:
        latest_hex = _rpc_any(urls, "eth_blockNumber", [])
        latest = int(latest_hex, 16)
        meta["latest_block"] = latest
    except Exception as e:
        print(f"[WARN] Falha ao obter blockNumber: {e}")
        meta["error"] = str(e)
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        return []

    out: List[Dict[str, Any]] = []
    cache: Dict[str, Dict[str, Any]] = _load_cache(data_dir)

    start_bn = max(0, latest - blocks_back + 1)
    meta["start_block"] = start_bn

    for bn in range(latest, start_bn - 1, -1):
        try:
            block = _rpc_any(urls, "eth_getBlockByNumber", [_to_hex(bn), True])
        except Exception as e:
            print(f"[WARN] Falha ao ler bloco {bn}: {e}")
            continue

        ts = _hex_to_int(block.get("timestamp", "0x0"))
        ts_iso = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

        txs = block.get("transactions", []) or []
        for tx in txs:
            # Filtros rápidos de origem/destino
            if not _passes_filters(tx, allow_from, allow_to):
                continue

            # Filtros de tipo/valor
            input_data = tx.get("input", "0x") or "0x"
            selector = input_data[:10].lower()
            val_wei = _hex_to_int(tx.get("value", "0x0"))

            if only_erc20 and selector not in (SIG_TRANSFER, SIG_APPROVE):
                continue
            if (input_data in ("0x", "0x0")) and val_wei > 0 and (val_wei < eth_min_wei):
                continue

            try:
                out.append(_decode_tx(urls, tx, ts_iso, cache))
            except Exception as e:
                print(f"[WARN] Falha ao decodificar tx em bloco {bn}: {e}")
                continue

            if len(out) >= max_tx:
                _save_cache(data_dir, cache)
                meta["collected"] = len(out)
                meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
                print(f"[ETH] chainId={meta['chain_id']} range={start_bn}-{latest} collected={len(out)} url={meta['selected_url']}")
                return out

    _save_cache(data_dir, cache)
    meta["collected"] = len(out)
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[ETH] chainId={meta['chain_id']} range={start_bn}-{latest} collected={len(out)} url={meta['selected_url']}")
    return out
