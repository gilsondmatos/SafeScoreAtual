import os
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import streamlit as st
import altair as alt

# PDF (opcional, fallback simples)
try:
    from fpdf import FPDF  # type: ignore
except Exception:
    FPDF = None  # type: ignore

DATA_DIR = Path("app/data")

# ---- PROTEÇÃO: só configurar a página 1x por sessão e como 1º comando do Streamlit
if "_cfg_done" not in st.session_state:
    try:
        st.set_page_config(page_title="SafeScore — Dashboard", layout="wide")
    finally:
        st.session_state["_cfg_done"] = True


# ----------------- HELPERS -----------------
def apply_secrets_to_env() -> None:
    """Carrega st.secrets (Streamlit Cloud) para o ambiente, se ainda não existir."""
    try:
        for k, v in st.secrets.items():
            if isinstance(v, (dict, list)):
                continue
            os.environ.setdefault(str(k), str(v))
    except Exception:
        pass


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


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
    for col in ("amount", "score", "penalty_total", "velocity_last_window"):
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


def kpi(label: str, value: Any) -> None:
    st.metric(label, value)


def bar_chart_token(df: pd.DataFrame) -> None:
    if df.empty:
        return
    c = (
        alt.Chart(df)
        .mark_bar()
        .encode(x=alt.X("token:N", sort="-y", title="Token"),
                y=alt.Y("count():Q", title="Qtde"))
        .properties(height=300)
    )
    st.altair_chart(c, use_container_width=True)


def contrib_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "contrib_pct" not in df.columns:
        return pd.DataFrame()
    rows = []
    for _, r in df.iterrows():
        c = r["contrib_pct"] or {}
        for k, v in c.items():
            rows.append({"tx_id": r.get("tx_id", ""), "rule": k, "pct": v})
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ----------------- COLETA INLINE -----------------
def _write_rows(path: Path, rows: List[Dict[str, Any]], header: List[str]) -> None:
    exists = path.exists()
    import csv
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        if not exists:
            w.writeheader()
        for r in rows:
            w.writerow(r)


def collect_and_score_now() -> int:
    """Coleta dados (ETH -> fallback mock), pontua e grava CSVs. Retorna N processados."""
    apply_secrets_to_env()
    ensure_data_dir()

    txs: List[Dict[str, Any]] = []
    chain_label = os.getenv("CHAIN_NAME", "ETH")

    # 1) tentar ETH
    try:
        import importlib
        ec = importlib.import_module("app.collectors.eth_collector")  # sem streamlit
        txs = getattr(ec, "load_from_eth")(DATA_DIR)
    except Exception as e:
        st.warning(f"Coletor ETH falhou: {e}. Usando dados mock.")
        # 2) fallback mock
        try:
            import importlib
            mc = importlib.import_module("app.collectors.mock_collector")  # sem streamlit
            txs = getattr(mc, "load_input_or_mock")(DATA_DIR)
            chain_label = "MOCK"
        except Exception as e2:
            st.error(f"Falha também no mock: {e2}")
            return 0

    if not txs:
        return 0

    # 3) scoring
    from app.engine.scoring import ScoreEngine
    prev_path = DATA_DIR / "transactions.csv"
    prev = []
    if prev_path.exists():
        prev = list(pd.read_csv(prev_path, dtype=str).to_dict(orient="records"))
    engine = ScoreEngine(data_dir=str(DATA_DIR), prev_transactions=prev, known_addresses=set())

    out_rows: List[Dict[str, Any]] = []
    for tx in txs:
        scored = engine.score_transaction(tx)
        hits: Dict[str, int] = scored.get("hits", {}) or {}
        penalty_total = int(sum(hits.values()))
        contrib_pct = {k: round((v / penalty_total) * 100, 1) for k, v in hits.items()} if penalty_total > 0 else {}
        explain_payload = {"weights": hits, "contrib_pct": contrib_pct}
        reasons_txt = "; ".join(scored.get("reasons") or [])
        out_rows.append({
            "tx_id": tx.get("tx_id", ""),
            "timestamp": tx.get("timestamp", ""),
            "from_address": tx.get("from_address", ""),
            "to_address": tx.get("to_address", ""),
            "amount": tx.get("amount", 0),
            "token": tx.get("token", ""),
            "method": tx.get("method", ""),
            "chain": chain_label,
            "is_new_address": "yes" if hits.get("new_address") else "no",
            "velocity_last_window": scored.get("velocity_last_window", 0),
            "score": scored["score"],
            "penalty_total": penalty_total,
            "reasons": reasons_txt,
            "explain": json.dumps(explain_payload, ensure_ascii=False),
        })

    header = ["tx_id", "timestamp", "from_address", "to_address", "amount", "token",
              "method", "chain", "is_new_address", "velocity_last_window", "score",
              "penalty_total", "reasons", "explain"]

    _write_rows(DATA_DIR / "transactions.csv", out_rows, header)
    _write_rows(DATA_DIR / f"transactions_{datetime.now().strftime('%Y%m%d')}.csv", out_rows, header)
    suf = chain_label.lower()
    _write_rows(DATA_DIR / f"transactions_{suf}.csv", out_rows, header)
    _write_rows(DATA_DIR / f"transactions_{suf}_{datetime.now().strftime('%Y%m%d')}.csv", out_rows, header)

    return len(out_rows)


# ----------------- PDF -----------------
def _pdf_from_rows(rows: pd.DataFrame, threshold: int, context_title: str) -> bytes:
    if FPDF is None:
        raise RuntimeError("FPDF não instalado")
    pdf = FPDF(orientation="P", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Arial", "B", 14)
    pdf.cell(0, 10, "Relatorio SafeScore - Transacoes Criticas", ln=True, align="C")

    pdf.set_font("Arial", "", 11)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    pdf.ln(2)
    pdf.multi_cell(0, 6, f"Data de geracao: {now}")
    pdf.multi_cell(0, 6, f"Contexto: {context_title}")
    pdf.multi_cell(0, 6, f"Limiar de alerta: score < {threshold}")
    pdf.multi_cell(0, 6, f"Total de criticos: {len(rows)}")
    pdf.ln(4)

    if len(rows) == 0:
        pdf.set_font("Arial", "I", 11)
        pdf.multi_cell(0, 6, "Nenhuma transacao critica encontrada no periodo.")
    else:
        pdf.set_font("Arial", "B", 11)
        pdf.cell(35, 8, "TX", border=1)
        pdf.cell(20, 8, "Score", border=1)
        pdf.cell(60, 8, "From", border=1)
        pdf.cell(60, 8, "To", border=1, ln=True)

        pdf.set_font("Arial", "", 10)
        for _, r in rows.iterrows():
            tx = str(r.get("tx_id", ""))[:18]
            sc = str(r.get("score", ""))
            fr = str(r.get("from_address", ""))
            to = str(r.get("to_address", ""))
            pdf.cell(35, 7, tx, border=1)
            pdf.cell(20, 7, sc, border=1)
            pdf.cell(60, 7, (fr[:34] + ("..." if len(fr) > 34 else "")), border=1)
            pdf.cell(60, 7, (to[:34] + ("..." if len(to) > 34 else "")), border=1, ln=True)

    out_path = DATA_DIR / "relatorio_dashboard.pdf"
    try:
        pdf.output(str(out_path))
    except PermissionError:
        out_path = DATA_DIR / f"relatorio_dashboard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        pdf.output(str(out_path))
    with open(out_path, "rb") as fh:
        return fh.read()


def generate_pdf_bytes(df_source: pd.DataFrame, threshold: int, context_title: str) -> bytes:
    try:
        import importlib
        gr = importlib.import_module("gerar_relatorio")
        rows = df_source.to_dict(orient="records")
        path = gr.build_pdf(rows, threshold)  # type: ignore
        return Path(path).read_bytes()
    except Exception:
        return _pdf_from_rows(df_source, threshold, context_title)


# ----------------- UI (FUNCIONAL) -----------------
apply_secrets_to_env()
ensure_data_dir()

st.title("SafeScore — Dashboard")
col_a, col_b = st.columns([4, 1])
with col_a:
    st.caption(f"Diretório de dados: {DATA_DIR.resolve()}")
with col_b:
    if st.button("⚡ Coletar agora (ETH)", key="collect_main"):
        with st.spinner("Coletando e pontuando..."):
            n = collect_and_score_now()
        if n > 0:
            st.success(f"Coleta concluída: {n} transações.")
            st.rerun()
        else:
            st.error("Não foi possível coletar dados. Verifique ETH_RPC_URL e limites do RPC.")

files = list_csvs()
if not files:
    st.info("Nenhum CSV encontrado em app/data. Use o botão acima ou rode `python main.py` localmente.")
    st.stop()

# --------- Filtros (sidebar) ---------
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

# --------- KPIs ---------
cols = st.columns(3)
with cols[0]:
    kpi("Transações (filtro)", len(df))
with cols[1]:
    kpi("Média de score", round(df["score"].mean(), 1) if not df.empty else 0)
with cols[2]:
    try:
        t_env = int(os.getenv("SCORE_ALERT_THRESHOLD", "50"))
    except Exception:
        t_env = 50
    kpi("Críticas (< limiar env)", int((df["score"] < t_env).sum()))

# --------- PDF ---------
st.subheader("Relatório (PDF)")
alert_input = st.number_input("Limiar de alerta (score < x)", min_value=0, max_value=100, value=t_env)
use_filters = st.checkbox("Usar filtros atuais no PDF", value=True)
if st.button("Gerar PDF de críticos", key="pdf_button"):
    base_df = df if use_filters else (df_all[df_all["chain"] == chain] if "chain" in df_all.columns else df_all)
    crit = base_df[base_df["score"] < alert_input].copy()
    try:
        pdf_bytes = generate_pdf_bytes(crit, alert_input, f"{fname} | chain={chain} | filtros={'on' if use_filters else 'off'}")
        st.success("PDF gerado. Use o botão abaixo para baixar.")
        st.download_button("Baixar PDF", data=pdf_bytes, file_name="relatorio_criticos.pdf", mime="application/pdf")
    except Exception as e:
        st.error(f"Falha ao gerar PDF: {e}")

# --------- Gráfico ---------
st.subheader("Distribuição por token")
bar_chart_token(df)

# --------- Tabela ---------
st.subheader("Transações")
cols_show = ["tx_id","timestamp","from_address","to_address","amount","token","method","chain","score","penalty_total"]
cols_show = [c for c in cols_show if c in df.columns]
st.dataframe(df[cols_show].sort_values(by="timestamp", ascending=False), use_container_width=True, height=420)

# --------- Contribuição ---------
if show_contrib:
    st.subheader("Contribuição por regra (%)")
    ctab = contrib_table(df)
    if ctab.empty:
        st.caption("Sem dados de contribuição disponíveis neste arquivo.")
    else:
        st.dataframe(ctab.sort_values(by=["tx_id","pct"], ascending=[True, False]), use_container_width=True, height=360)
