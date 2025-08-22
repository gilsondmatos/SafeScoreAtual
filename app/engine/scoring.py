from __future__ import annotations
import os
import csv
import json
from typing import Dict, Any, List, Set
from pathlib import Path

from .rules import RuleContext, DEFAULT_WEIGHTS, get_env_int

class ScoreEngine:
    def __init__(
        self,
        data_dir: str,
        prev_transactions: List[Dict[str, Any]] | None = None,
        known_addresses: Set[str] | None = None,
    ):
        self.data_dir = Path(data_dir)
        self.prev_transactions = prev_transactions or []
        self.known_addresses = known_addresses or set()

        # Carregar listas
        self.blacklist = self._load_single_col_csv("blacklist.csv")
        self.watchlist = self._load_single_col_csv("watchlist.csv")
        self.sensitive_tokens = self._load_single_col_csv("sensitive_tokens.csv", upper=True)
        self.sensitive_methods = self._load_single_col_csv("sensitive_methods.csv", upper=True)

        # Pesos (permite override via weights.json)
        self.weights = self._load_weights()

        # Parâmetros de regras
        amount_threshold = float(os.getenv("AMOUNT_THRESHOLD", "10000"))
        velocity_window_min = get_env_int("VELOCITY_WINDOW_MIN", 10)
        velocity_max_tx = get_env_int("VELOCITY_MAX_TX", 5)

        self.ctx = RuleContext(
            blacklist=self.blacklist,
            watchlist=self.watchlist,
            known_addresses=self.known_addresses,
            sensitive_tokens=self.sensitive_tokens,
            sensitive_methods=self.sensitive_methods,
            prev_transactions=self.prev_transactions,
            weights=self.weights,
            amount_threshold=amount_threshold,
            velocity_window_min=velocity_window_min,
            velocity_max_tx=velocity_max_tx,
        )

    def _load_single_col_csv(self, filename: str, upper: bool = False) -> Set[str]:
        p = self.data_dir / filename
        if not p.exists():
            return set()
        out: Set[str] = set()
        with p.open("r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            # aceita com ou sem header
            if header:
                for row in reader:
                    if not row: continue
                    v = (row[0] or "").strip()
                    if not v: continue
                    out.add(v.upper() if upper else v)
            else:
                # reabrir sem pular primeira linha
                f.seek(0)
                for row in csv.reader(f):
                    if not row: continue
                    v = (row[0] or "").strip()
                    if not v: continue
                    out.add(v.upper() if upper else v)
        return out

    def _load_weights(self) -> Dict[str, int]:
        """Lê app/data/weights.json e faz merge com DEFAULT_WEIGHTS."""
        fp = self.data_dir / "weights.json"
        weights = DEFAULT_WEIGHTS.copy()
        if fp.exists():
            try:
                with fp.open("r", encoding="utf-8") as f:
                    data = json.load(f) or {}
                for k, v in (data.items() if isinstance(data, dict) else []):
                    try:
                        weights[str(k)] = int(v)
                    except Exception:
                        pass
            except Exception:
                # se houver erro, usa apenas DEFAULT_WEIGHTS
                pass
        return weights

    def score_transaction(self, tx: Dict[str, Any]) -> Dict[str, Any]:
        """
        Score começa em 100 e sofre penalidades por regra.
        Retorna: { score: int, reasons: [str], hits: {regra: peso}, velocity_last_window: int }
        """
        score = 100
        hits: Dict[str, int] = {}
        reasons: List[str] = []

        # Regras
        self.ctx.r_blacklist(tx, hits, reasons)
        self.ctx.r_watchlist(tx, hits, reasons)
        self.ctx.r_high_amount(tx, hits, reasons)
        self.ctx.r_unusual_hour(tx, hits, reasons)
        self.ctx.r_new_address(tx, hits, reasons)
        velocity_count = self.ctx.r_velocity(tx, hits, reasons)
        self.ctx.r_sensitive_token(tx, hits, reasons)
        self.ctx.r_sensitive_method(tx, hits, reasons)

        # Agregar penalidades
        for _, w in hits.items():
            score -= int(w)

        if score < 0: score = 0
        if score > 100: score = 100

        return {
            "score": int(score),
            "reasons": reasons,
            "hits": hits,
            "velocity_last_window": velocity_count,
        }
