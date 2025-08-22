import os
import json
import pandas as pd
import streamlit as st
from pathlib import Path
from datetime import datetime

from eth_collector import load_from_eth
from mock_collector import load_input_or_mock
from app.engine.scoring import ScoreEngine

DATA_DIR = Path("app/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

st.set_page_config(page_title="SafeScore Dashboard", layout="wide")

# -------- Funções utilitárias --------
def list_csvs():
    return sorted(DATA_DIR.glob("transactions*.csv"))

def load_csv(path: Path):
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, dtype=str)
    for col in ("amount", "score", "penalty_total"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    if "explain" in df.columns:
        def parse(x):
            try:
                return json.loads(str(x)).get("contrib_pct", {})
            except Exception:
                return {}
        df["contrib_pct"] = df["explain"].apply(parse)
    return df

def run_collect_and_score():
    """Executa coleta ETH → fallback mock → scoring → salva CSV"""
    try:
        st.info("⏳ Coletando transações da Ethereum...")
        txs = load_from_eth(DATA_DIR)
        chain = "ETH"
    except Exception as e:
        st.warning(f"Falha na ETH ({e}), usando MOCK.")
        txs = load_input_or_mock(DATA_DIR)
        chain = "MOCK"

    # Scoring
    engine = ScoreEngine(data_dir=str(DATA_DIR), prev_transactions=[], known_addresses=set())
    scored = [engine.score_transaction(tx) for tx in txs]

    # Salvar CSV
    today = datetime.now().strftime("%Y%m%d_%H%M%S")
    fname = f"transactions_{chain.lower()}_{today}.csv"
    fpath = DATA_DIR / fname
    pd.DataFrame(scored).to_csv(fpath, index=False)
    return fpath

# -------- Interface --------
st.title("🔎 SafeScore — Antifraude")

if st.button("⚡ Coletar da Ethereum", key="collect_eth"):
    fpath = run_collect_and_score()
    st.success(f"Coleta concluída: {fpath.name}")

files = list_csvs()
if not files:
    st.info("Nenhum CSV encontrado. Clique no botão acima para coletar.")
    st.stop()

# Selecionar arquivo
default_file = files[-1]
fsel = st.sidebar.selectbox("Arquivo de transações", files, index=files.index(default_file))
df = load_csv(fsel)

if df.empty:
    st.warning("Arquivo vazio.")
    st.stop()

# KPIs
cols = st.columns(3)
with cols[0]:
    st.metric("Transações", len(df))
with cols[1]:
    st.metric("Média Score", round(df["score"].mean(), 1) if "score" in df else 0)
with cols[2]:
    limiar = int(os.getenv("SCORE_ALERT_THRESHOLD", "50"))
    st.metric("Críticas (< limiar)", int((df["score"] < limiar).sum()))

# Tabela
st.subheader("Transações")
cols_show = ["tx_id","timestamp","from_address","to_address","amount","token","method","chain","score"]
cols_show = [c for c in cols_show if c in df.columns]
st.dataframe(df[cols_show].sort_values(by="timestamp", ascending=False), use_container_width=True, height=420)
