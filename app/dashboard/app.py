# app.py
import os
import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st
import altair as alt

from eth_collector import collect_eth
from mock_collector import collect_mock

# ----------------- Configuração -----------------
st.set_page_config(page_title="SafeScore Dashboard", layout="wide")
DATA_DIR = Path("app/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ----------------- Funções utilitárias -----------------
def list_csvs():
    files = sorted(DATA_DIR.glob("transactions*.csv"))
    return [f.name for f in files]

def pick_default_csv(files):
    today = datetime.now().strftime("%Y%m%d")
    preferred = f"transactions_eth_{today}.csv"
    if preferred in files:
        return preferred
    eth_files = [f for f in files if f.startswith("transactions_eth_")]
    if eth_files:
        return sorted(eth_files)[-1]
    if files:
        return files[-1]
    return None

def load_csv(fname):
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
                j = json.loads(str(x))
                return j.get("contrib_pct", {})
            except Exception:
                return {}
        df["contrib_pct"] = df["explain"].apply(parse_contrib)
    return df

def kpi(label, value):
    st.metric(label, value)

def bar_chart_token(df):
    if df.empty or "token" not in df.columns:
        return
    c = (
        alt.Chart(df)
        .mark_bar()
        .encode(x=alt.X("token:N", sort="-y"), y="count():Q")
        .properties(height=320)
    )
    st.altair_chart(c, use_container_width=True)

def contrib_table(df):
    if df.empty or "contrib_pct" not in df.columns:
        return pd.DataFrame()
    rows = []
    for _, r in df.iterrows():
        c = r["contrib_pct"] or {}
        for k, v in c.items():
            rows.append({"tx_id": r.get("tx_id",""), "rule": k, "pct": v})
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)

# ----------------- Botão de coleta -----------------
def run_collect_and_score():
    try:
        st.info("Coletando transações reais da Ethereum...")
        fpath = collect_eth(DATA_DIR)
        return fpath
    except Exception as e:
        st.error(f"Coleta Ethereum falhou: {e}. Usando dados mock.")
        return collect_mock(DATA_DIR)

# ----------------- Layout principal -----------------
st.title("🔎 SafeScore — Dashboard")
st.caption(f"Diretório de dados: {DATA_DIR}")

files = list_csvs()

# Caso inicial: nenhum CSV ainda
if not files:
    st.warning("Nenhum CSV encontrado em app/data. Clique abaixo para coletar dados.")
    if st.button("⚡ Coletar agora (Ethereum)", key="collect_main"):
        fpath = run_collect_and_score()
        if fpath:
            st.success(f"Coleta concluída: {fpath.name}")
            files = list_csvs()
        else:
            st.stop()
    else:
        st.stop()

# Seleção de arquivo
default_file = pick_default_csv(files)
fname = st.sidebar.selectbox(
    "Arquivo de transações", files, 
    index=files.index(default_file) if default_file in files else 0
)

df_all = load_csv(fname)
if df_all.empty:
    st.error("Arquivo vazio.")
    st.stop()

# Filtro por chain
available_chains = sorted(df_all["chain"].dropna().unique()) if "chain" in df_all.columns else ["ETH"]
default_chain = os.getenv("DASHBOARD_DEFAULT_CHAIN", "ETH")
default_chain_idx = available_chains.index(default_chain) if default_chain in available_chains else 0
chain = st.sidebar.selectbox("Fonte (chain)", available_chains, index=default_chain_idx)

df = df_all[df_all["chain"] == chain] if "chain" in df_all.columns else df_all.copy()

# Outros filtros
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

# --------- KPIs ---------
cols = st.columns(3)
with cols[0]:
    kpi("Transações (filtro)", len(df))
with cols[1]:
    kpi("Média de score", round(df["score"].mean(), 1) if not df.empty else 0)
with cols[2]:
    try:
        t = int(os.getenv("SCORE_ALERT_THRESHOLD", "50"))
    except Exception:
        t = 50
    kpi("Críticas (< limiar)", int((df["score"] < t).sum()))

# --------- Gráfico por token ---------
st.subheader("Distribuição por token")
bar_chart_token(df)

# --------- Tabela principal ---------
st.subheader("Transações")
cols_show = ["tx_id","timestamp","from_address","to_address","amount","token","method","chain","score","penalty_total"]
cols_show = [c for c in cols_show if c in df.columns]
st.dataframe(df[cols_show].sort_values(by="timestamp", ascending=False), use_container_width=True, height=420)

# --------- Contribuição por regra ---------
if show_contrib:
    st.subheader("Contribuição por regra (%)")
    ctab = contrib_table(df)
    if ctab.empty:
        st.caption("Sem dados de contribuição disponíveis neste arquivo.")
    else:
        st.dataframe(ctab.sort_values(by=["tx_id","pct"], ascending=[True, False]), use_container_width=True, height=360)
