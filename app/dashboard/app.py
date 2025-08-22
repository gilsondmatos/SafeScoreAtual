import os
import json
import math
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st
import altair as alt

DATA_DIR = Path("app/data")
st.set_page_config(page_title="SafeScore Dashboard", layout="wide")

def list_csvs():
    files = sorted(DATA_DIR.glob("transactions*.csv"))
    return [f.name for f in files]

def pick_default_csv(files):
    # Preferir arquivo diário da ETH se existir
    today = datetime.now().strftime("%Y%m%d")
    preferred = f"transactions_eth_{today}.csv"
    if preferred in files:
        return preferred
    # fallback: qualquer transactions_eth*.csv mais recente
    eth_files = [f for f in files if f.startswith("transactions_eth_")]
    if eth_files:
        return sorted(eth_files)[-1]
    # fallback geral
    if files:
        return files[-1]
    return None

def load_csv(fname):
    path = DATA_DIR / fname
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, dtype=str)
    # normalizar tipos básicos (suave)
    for col in ("amount","score","penalty_total","velocity_last_window"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    # parse explain -> contrib_pct
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
    if df.empty:
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
    # explode em linhas chave/valor
    rows = []
    for _, r in df.iterrows():
        c = r["contrib_pct"] or {}
        for k, v in c.items():
            rows.append({"tx_id": r.get("tx_id",""), "rule": k, "pct": v})
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)

# --------- Sidebar ---------
st.sidebar.header("Filtros")

files = list_csvs()
if not files:
    st.info("Nenhum CSV encontrado em app/data. Rode `python main.py`.")
    st.stop()

default_file = pick_default_csv(files)
fname = st.sidebar.selectbox("Arquivo de transações", files, index=files.index(default_file) if default_file in files else 0)

df_all = load_csv(fname)
if df_all.empty:
    st.warning("Arquivo vazio.")
    st.stop()

# Filtro por chain (padrão: ETH)
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
st.title("🔎 SafeScore — Antifraude")
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

# --------- Parâmetros ---------
st.sidebar.markdown("### Parâmetros")
alert_input = st.sidebar.number_input("Limiar de alerta (score < x)", min_value=0, max_value=100, value=int(os.getenv("SCORE_ALERT_THRESHOLD", "50")))
st.sidebar.caption("Dica: ajuste aqui e gere o PDF na seção abaixo.")

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
