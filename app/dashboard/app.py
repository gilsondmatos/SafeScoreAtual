import os
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import streamlit as st
import altair as alt

from app.collectors.eth_collector import load_from_eth
from app.collectors.mock_collector import load_input_or_mock
from app.scoring import ScoreEngine

DATA_DIR = Path("app/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# deve ser a primeira chamada do app
st.set_page_config(page_title="SafeScore Dashboard", layout="wide")

# ----------------- util -----------------
def list_csvs() -> List[str]:
    return sorted([f.name for f in DATA_DIR.glob("transactions*.csv")])

def pick_default_csv(files: List[str]) -> str | None:
    today = datetime.now().strftime("%Y%m%d")
    preferred = f"transactions_eth_{today}.csv"
    if preferred in files:
        return preferred
    eth_files = [f for f in files if f.startswith("transactions_eth_")]
    if eth_files:
        return sorted(eth_files)[-1]
    return files[-1] if files else None

def load_csv(fname: str) -> pd.DataFrame:
    path = DATA_DIR / fname
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, dtype=str)
    for col in ("amount","score","penalty_total","velocity_last_window"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    if "explain" in df.columns:
        def parse_contrib(x):
            try:
                j = json.loads(str(x));  return j.get("contrib_pct", {})
            except Exception:
                return {}
        df["contrib_pct"] = df["explain"].apply(parse_contrib)
    return df

def _explain_from_hits(hits: Dict[str,int]) -> Dict[str,Any]:
    tot = sum(int(v) for v in hits.values()) or 0
    return {
        "weights": hits,
        "contrib_pct": {k: round((int(v)/tot)*100,1) for k,v in hits.items()} if tot>0 else {}
    }

def collect_and_score() -> Path | None:
    """Coleta ETH -> fallback mock, faz scoring e salva CSVs."""
    try:
        txs = load_from_eth(DATA_DIR)
        chain = "ETH"
    except Exception as e:
        st.warning(f"Coletor ETH falhou ({e}). Usando mock.")
        txs = load_input_or_mock(DATA_DIR)
        chain = "MOCK"
    if not txs:
        return None

    prev_path = DATA_DIR / "transactions.csv"
    prev = []
    if prev_path.exists():
        prev = list(pd.read_csv(prev_path, dtype=str).to_dict(orient="records"))

    engine = ScoreEngine(data_dir=str(DATA_DIR), prev_transactions=prev, known_addresses=set())
    rows: List[Dict[str,Any]] = []
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
            "explain": json.dumps(_explain_from_hits(hits), ensure_ascii=False),
        })

    df = pd.DataFrame(rows)
    today = datetime.now().strftime("%Y%m%d")
    f_daily = DATA_DIR / f"transactions_{chain.lower()}_{today}.csv"
    f_main  = DATA_DIR / "transactions.csv"
    df.to_csv(f_daily, index=False)
    df.to_csv(f_main, index=False)
    return f_daily

# ----------------- UI -----------------
st.title("🔎 SafeScore — Antifraude")
st.caption(f"Diretório de dados: {DATA_DIR.resolve()}")

files = list_csvs()
if not files:
    st.info("Nenhum CSV encontrado. Clique para coletar agora.")
    if st.button("⚡ Coletar agora (ETH)", key="collect_eth_first"):
        p = collect_and_score()
        if p:
            st.success(f"Coleta concluída: {p.name}")
            files = list_csvs()
        else:
            st.error("Falha na coleta. Verifique ETH_RPC_URL.")
    if not files:
        st.stop()

st.sidebar.header("Filtros")

default_file = pick_default_csv(files)
fname = st.sidebar.selectbox("Arquivo de transações", files, index=files.index(default_file) if default_file in files else 0)

df_all = load_csv(fname)
if df_all.empty:
    st.warning("Arquivo vazio.")
    st.stop()

available_chains = sorted(df_all["chain"].dropna().unique()) if "chain" in df_all.columns else ["ETH"]
default_chain = os.getenv("DASHBOARD_DEFAULT_CHAIN", "ETH")
default_chain_idx = available_chains.index(default_chain) if default_chain in available_chains else 0
chain = st.sidebar.selectbox("Fonte (chain)", available_chains, index=default_chain_idx)

df = df_all[df_all["chain"] == chain] if "chain" in df_all.columns else df_all.copy()

tokens = ["(todos)"] + sorted([t for t in df["token"].dropna().unique()]) if "token" in df.columns else ["(todos)"]
ftoken = st.sidebar.selectbox("Token", tokens, index=0)
addr_filter = st.sidebar.text_input("Filtro por endereço (contém)")
score_min, score_max = st.sidebar.slider("Faixa de score", 0, 100, (0, 100))
show_contrib = st.sidebar.checkbox("Mostrar contribuição por regra (%)", value=True)

if ftoken != "(todos)":
    df = df[df["token"] == ftoken]
if addr_filter:
    m = df["from_address"].astype(str).str.contains(addr_filter, case=False, na=False) | \
        df["to_address"].astype(str).str.contains(addr_filter, case=False, na=False)
    df = df[m]
df = df[(df["score"] >= score_min) & (df["score"] <= score_max)]

cols = st.columns(4)
with cols[0]:
    st.metric("Transações (filtro)", len(df))
with cols[1]:
    st.metric("Média de score", round(df["score"].mean(), 1) if not df.empty else 0)
with cols[2]:
    t_env = int(os.getenv("SCORE_ALERT_THRESHOLD", "50"))
    st.metric("Críticas (< limiar env)", int((df["score"] < t_env).sum()))
with cols[3]:
    if st.button("⚡ Coletar novamente (ETH)", key="collect_eth_again"):
        p = collect_and_score()
        if p:
            st.success(f"Coleta concluída: {p.name}")
            st.experimental_rerun()

st.subheader("Distribuição por token")
if not df.empty and "token" in df.columns:
    chart = (alt.Chart(df).mark_bar().encode(x=alt.X("token:N", sort="-y"), y="count():Q").properties(height=300))
    st.altair_chart(chart, use_container_width=True)

st.subheader("Transações")
cols_show = ["tx_id","timestamp","from_address","to_address","amount","token","method","chain","score","penalty_total"]
cols_show = [c for c in cols_show if c in df.columns]
st.dataframe(df[cols_show].sort_values(by="timestamp", ascending=False), use_container_width=True, height=420)

if show_contrib:
    st.subheader("Contribuição por regra (%)")
    if "contrib_pct" in df.columns:
        rows = []
        for _, r in df.iterrows():
            for k, v in (r["contrib_pct"] or {}).items():
                rows.append({"tx_id": r.get("tx_id",""), "rule": k, "pct": v})
        if rows:
            st.dataframe(pd.DataFrame(rows).sort_values(by=["tx_id","pct"], ascending=[True, False]),
                         use_container_width=True, height=360)
        else:
            st.caption("Sem dados de contribuição neste arquivo.")
    else:
        st.caption("Sem coluna de contribuição (explain).")
