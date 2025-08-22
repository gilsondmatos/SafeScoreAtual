from __future__ import annotations
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Set

# Pesos padrão (penalidades subtraídas do score que começa em 100)
DEFAULT_WEIGHTS = {
    "blacklist": 60,
    "watchlist": 30,
    "high_amount": 25,
    "unusual_hour": 15,
    "new_address": 20,
    "velocity": 20,
    "sensitive_token": 15,
    "sensitive_method": 15,
}

def get_env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

class RuleContext:
    def __init__(
        self,
        blacklist: Set[str],
        watchlist: Set[str],
        known_addresses: Set[str],
        sensitive_tokens: Set[str],
        sensitive_methods: Set[str],
        prev_transactions: List[Dict[str, Any]],
        weights: Dict[str, int] | None = None,
        amount_threshold: float = 10000.0,
        velocity_window_min: int = 10,
        velocity_max_tx: int = 5,
    ):
        self.blacklist = {x.lower() for x in blacklist}
        self.watchlist = {x.lower() for x in watchlist}
        self.known_addresses = {x.lower() for x in known_addresses}
        self.sensitive_tokens = {x.upper() for x in sensitive_tokens}
        self.sensitive_methods = {x.upper() for x in sensitive_methods}
        self.prev_transactions = prev_transactions or []
        self.weights = weights or DEFAULT_WEIGHTS
        self.amount_threshold = amount_threshold
        self.velocity_window_min = velocity_window_min
        self.velocity_max_tx = velocity_max_tx

    def r_blacklist(self, tx: Dict[str, Any], hits: Dict[str, int], reasons: List[str]):
        w = self.weights.get("blacklist", 0)
        if not w: return
        a = (tx.get("from_address","") or "").lower()
        b = (tx.get("to_address","") or "").lower()
        if a in self.blacklist or b in self.blacklist:
            hits["blacklist"] = w
            reasons.append("Endereço em blacklist")

    def r_watchlist(self, tx: Dict[str, Any], hits: Dict[str, int], reasons: List[str]):
        w = self.weights.get("watchlist", 0)
        if not w: return
        a = (tx.get("from_address","") or "").lower()
        b = (tx.get("to_address","") or "").lower()
        if a in self.watchlist or b in self.watchlist:
            hits["watchlist"] = w
            reasons.append("Endereço em watchlist")

    def r_high_amount(self, tx: Dict[str, Any], hits: Dict[str, int], reasons: List[str]):
        w = self.weights.get("high_amount", 0)
        if not w: return
        try:
            amount = float(tx.get("amount", 0) or 0)
        except Exception:
            amount = 0.0
        if amount >= float(self.amount_threshold):
            hits["high_amount"] = w
            reasons.append(f"Valor alto (≥ {self.amount_threshold})")

    def r_unusual_hour(self, tx: Dict[str, Any], hits: Dict[str, int], reasons: List[str]):
        w = self.weights.get("unusual_hour", 0)
        if not w: return
        ts = tx.get("timestamp") or ""
        try:
            if ts.endswith("Z"):
                dt = datetime.fromisoformat(ts.replace("Z","+00:00"))
            else:
                dt = datetime.fromisoformat(ts)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
            hour = dt.hour
        except Exception:
            hour = 0
        if hour < 6:
            hits["unusual_hour"] = w
            reasons.append("Horário incomum (madrugada)")

    def r_new_address(self, tx: Dict[str, Any], hits: Dict[str, int], reasons: List[str]):
        w = self.weights.get("new_address", 0)
        if not w: return
        a = (tx.get("from_address","") or "").lower()
        if a and a not in self.known_addresses:
            hits["new_address"] = w
            reasons.append("Endereço remetente novo")

    def r_velocity(self, tx: Dict[str, Any], hits: Dict[str, int], reasons: List[str]) -> int:
        w = self.weights.get("velocity", 0)
        if not w: return 0
        a = (tx.get("from_address","") or "").lower()
        ts = tx.get("timestamp") or ""
        try:
            if ts.endswith("Z"):
                now_dt = datetime.fromisoformat(ts.replace("Z","+00:00"))
            else:
                now_dt = datetime.fromisoformat(ts)
                if now_dt.tzinfo is None:
                    now_dt = now_dt.replace(tzinfo=timezone.utc)
        except Exception:
            now_dt = datetime.now(timezone.utc)

        window_start = now_dt - timedelta(minutes=int(self.velocity_window_min))
        count = 0
        for p in self.prev_transactions:
            if (p.get("from_address","") or "").lower() != a:
                continue
            pts = p.get("timestamp") or ""
            try:
                if pts.endswith("Z"):
                    pdt = datetime.fromisoformat(pts.replace("Z","+00:00"))
                else:
                    pdt = datetime.fromisoformat(pts)
                    if pdt.tzinfo is None:
                        pdt = pdt.replace(tzinfo=timezone.utc)
            except Exception:
                continue
            if window_start <= pdt <= now_dt:
                count += 1

        if count >= int(self.velocity_max_tx):
            hits["velocity"] = w
            reasons.append(f"Velocidade anômala ({count} tx em {self.velocity_window_min} min)")
        return count

    def r_sensitive_token(self, tx: Dict[str, Any], hits: Dict[str, int], reasons: List[str]):
        w = self.weights.get("sensitive_token", 0)
        if not w: return
        token = (tx.get("token","") or "").upper()
        if token and token in self.sensitive_tokens:
            hits["sensitive_token"] = w
            reasons.append(f"Token sensível ({token})")

    def r_sensitive_method(self, tx: Dict[str, Any], hits: Dict[str, int], reasons: List[str]):
        w = self.weights.get("sensitive_method", 0)
        if not w: return
        method = (tx.get("method","") or "").upper()
        if method and method in self.sensitive_methods:
            hits["sensitive_method"] = w
            reasons.append(f"Método sensível ({method})")
