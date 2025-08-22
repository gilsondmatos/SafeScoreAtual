import os
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import streamlit as st
import altair as alt

# PDF (inline fallback)
try:
    from fpdf import FPDF  # type: ignore
except Exception:
    FPDF = None  # type: ignore

# ================== CONFIG ==================
DATA_DIR = Path("app/data")

# Proteção: alguns ambientes podem re-importar o módulo;
# chamar set_page_config uma única vez e o mais cedo possível.
if not st.session_state.get("_page_cfg_set"):
    try:
        st.set_page_config(page_title="SafeScore Dashboard", layout="wide")
    except Exception:
        pass
    st.session_state["_page_cfg_set"] = True

# ---------- estilos (cores) ----------
PRIMARY = "#6EE7F9"
ACCENT = "#22D3EE"
INFO = "#E0F2FE"
TEXT_DARK = "#0F172A"

st.markdown(
    f"""
<style>
.small-pill {{
  display:inline-block; padding:4px 10px; border-radius:999px;
  font-size:12px; font-weight:600; margin-right:6px;
  color:{TEXT_DARK}; background:{INFO};
}}
.explain {{
  border-left: 6px solid {ACCENT};
  background: linear-gradient(90deg, rgba(34,211,238,0.08) 0%, rgba(34,211,238,0.02) 100%);
  padding:10px 12px; border-radius:12px; margin:6px 0 14px 0; color:{TEXT_DARK};
  font-size:13px;
}}
.kpi-card {{
  background: linear-gradient(160deg, rgba(110,231,249,0.18) 0%, rgba(34,211,238,0.10) 100%);
  padding:16px; border-radius:16px; text-align:center; border:1px solid rgba(34,211,238,0.2);
}}
.kpi-card h3 {{ margin:0; font-size:15px; color:{TEXT_DARK}; }}
.kpi-card .v {{ font-size:26px; font-weight:800; color:#0ea5e9; }}
.section-title {{ font-weight:800; font-size:20px; margin:4px 0 6px 0; }}
</style>
""",
    unsafe_allow_html=True,
)

# ================== HELPERS ==================
def apply_secrets_to_env() -> None:
    """Carrega st.secrets (Streamlit Cloud) para o ambiente, se não setado."""
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

def kpi(label: str, value: Any):
    st.markdown(
        f"""<div class="kpi-card"><h3>{label}</h3><div class="v">{value}</div></div>""",
        unsafe_allow_html=True,
    )

def bar_chart_token(df: pd.DataFrame):
    if df.empty:
        return
    c = (
        alt.Chart(df)
        .mark_bar()
        .encode(x=alt.X("token:N", sort="-y", title="Token"),
                y=alt.Y("count():Q", title="Qtde de transações"))
        .properties(height=320)
    )
    st.altair_chart(c, use_container_width=True)

def contrib_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "contrib_pct" not in df.columns:
        return pd.DataFrame()
    rows = []
    for _, r in df.iterrows():
        c = r["contrib_pct"] or {}
        for k, v in c.items():
            rows.append({"tx_id": r.get("tx_id",""), "rule": k, "pct": v})
    return pd.DataFrame(rows) if rows else pd.DataFrame()

def explain(msg: str):
    st.markdown(f"""<div class="explain">💡 {msg}</div>""", unsafe_allow_html=True)

def safe_text(text: str) -> str:
    if text is None: return ""
    t = str(text)
    t = t.replace("—","-").replace("–","-").replace("…","...").replace("≥",">=").replace("≤","<=").replace("•","-")
    try:
        return t.encode("latin-1","replace").decode("latin-1")
    except Exception:
        return t

# ============== PIPELINE INLINE (Cloud) ==============
def _write_rows(path: Path, rows: List[Dict[str, Any]], header: List[str]) -> None:
    exists = path.exists()
    import csv
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        if not exists: w.writeheader()
        for r in rows: w.writerow(r)

def _score_rows(txs: List[Dict[str, Any]], chain_label: str) -> List[Dict[str, Any]]:
    from app.engine.scoring import ScoreEngine  # lazy import
    prev_path = DATA_DIR / "transactions.csv"
    prev = []
    if prev_path.exists():
        prev = list(pd.read_csv(prev_path, dtype=str).to_dict(orient="records"))
    engine = ScoreEngine(data_dir=str(DATA_DIR), prev_transactions=prev, known_addresses=set())

    out = []
    for tx in txs:
        scored = engine.score_transaction(tx)
        hits: Dict[str, int] = scored.get("hits", {}) or {}
        penalty_total = int(sum(hits.values()))
        contrib_pct = {k: round((v / penalty_total) * 100, 1) for k, v in hits.items()} if penalty_total > 0 else {}
        explain_payload = {"weights": hits, "contrib_pct": contrib_pct}
        reasons_txt = safe_text("; ".join(scored.get("reasons") or []))
        out.append({
            "tx_id": tx.get("tx_id",""),
            "timestamp": tx.get("timestamp",""),
            "from_address": tx.get("from_address",""),
            "to_address": tx.get("to_address",""),
            "amount": tx.get("amount",0),
            "token": tx.get("token",""),
            "method": tx.get("method",""),
            "chain": chain_label,
            "is_new_address": "yes" if hits.get("new_address") else "no",
            "velocity_last_window": scored.get("velocity_last_window", 0),
            "score": scored["score"],
            "penalty_total": penalty_total,
            "reasons": reasons_txt,
            "explain": json.dumps(explain_payload, ensure_ascii=False),
        })
    return out

def collect_and_score_now() -> int:
    """
    Executa coleta ETH + scoring e grava CSVs básicos.
    Retorna a quantidade de transações processadas.
    """
    apply_secrets_to_env()
    ensure_data_dir()

    txs: List[Dict[str, Any]] = []
    chain_label = os.getenv("CHAIN_NAME", "ETH")

    # 1) tentar ETH
    try:
        import importlib
        ec = importlib.import_module("app.collectors.eth_collector")
        txs = getattr(ec, "load_from_eth")(DATA_DIR)
    except Exception as e:
        st.warning(f"Coletor ETH falhou ({e}). Gerando dados mock para demo.")
        # 2) fallback mock
        try:
            import importlib
            mc = importlib.import_module("app.collectors.mock_collector")
            txs = getattr(mc, "load_input_or_mock")(DATA_DIR)
            chain_label = "MOCK"
        except Exception as e2:
            st.error(f"Falha também no mock: {e2}")
            return 0

    if not txs:
        return 0

    # score + salvar
    out_rows = _score_rows(txs, chain_label)
    header = ["tx_id","timestamp","from_address","to_address","amount","token","method","chain",
              "is_new_address","velocity_last_window","score","penalty_total","reasons","explain"]
    _write_rows(DATA_DIR / "transactions.csv", out_rows, header)
    _write_rows(DATA_DIR / f"transactions_{datetime.now().strftime('%Y%m%d')}.csv", out_rows, header)
    suf = chain_label.lower()
    _write_rows(DATA_DIR / f"transactions_{suf}.csv", out_rows, header)
    _write_rows(DATA_DIR / f"transactions_{suf}_{datetime.now().strftime('%Y%m%d')}.csv", out_rows, header)
    return len(out_rows)

# ============== PDF ==============
def _pdf_from_rows(rows: pd.DataFrame, threshold: int, context_title: str) -> bytes:
    if FPDF is None:
        raise RuntimeError("FPDF não instalado")
    pdf = FPDF(orientation="P", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Arial", "B", 14)
    pdf.cell(0, 10, safe_text("Relatorio SafeScore - Transacoes Criticas"), ln=True, align="C")

    pdf.set_font("Arial", "", 11)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    pdf.ln(2)
    pdf.multi_cell(0, 6, safe_text(f"Data de geracao: {now}"))
    pdf.multi_cell(0, 6, safe_text(f"Contexto: {context_title}"))
    pdf.multi_cell(0, 6, safe_text(f"Limiar de alerta: score < {threshold}"))
    pdf.multi_cell(0, 6, safe_text(f"Total de criticos: {len(rows)}"))
    pdf.ln(4)

    if len(rows) == 0:
        pdf.set_font("Arial", "I", 11)
        pdf.multi_cell(0, 6, safe_text("Nenhuma transacao critica encontrada no periodo."))
    else:
        pdf.set_font("Arial", "B", 11)
        pdf.cell(35, 8, "TX", border=1)
        pdf.cell(20, 8, "Score", border=1)
        pdf.cell(60, 8, "From", border=1)
        pdf.cell(60, 8, "To", border=1, ln=True)

        pdf.set_font("Arial", "", 10)
        for _, r in rows.iterrows():
            tx = safe_text(str(r.get("tx_id",""))[:18])
            sc = safe_text(str(r.get("score","")))
            fr = safe_text(str(r.get("from_address","")))
            to = safe_text(str(r.get("to_address","")))
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

# ================== UI ==================
st.sidebar.header("Filtros")
apply_secrets_to_env()
ensure_data_dir()

files = list_csvs()
if not files:
    st.warning("Nenhum CSV encontrado em app/data.")
    # >>> chave única para o botão superior
    if st.button("⚡ Coletar agora (ETH)", key="collect_top"):
        with st.spinner("Coletando on-chain e gerando CSVs..."):
            n = collect_and_score_now()
        if n > 0:
            st.success(f"Coleta concluída ({n} transações). Recarregando…")
            st.rerun()
        else:
            st.error("Não foi possível coletar dados agora. Confira as secrets (ETH_RPC_URL etc.).")
    st.stop()

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

# Cabeçalho
st.title("🔎 SafeScore — Antifraude")
st.markdown(f"""
<span class="small-pill">Arquivo: {fname}</span>
<span class="small-pill">Chain: {chain}</span>
""", unsafe_allow_html=True)
st.markdown(f"""<div class="explain">💡 Se o app reiniciar vazio, clique em <b>Coletar agora (ETH)</b> para popular <code>app/data</code> neste ambiente.</div>""", unsafe_allow_html=True)

# KPIs
c1, c2, c3, c4 = st.columns(4)
with c1: kpi("Transações (filtro)", len(df))
with c2: kpi("Média de score", round(df["score"].mean(), 1) if not df.empty else 0)
with c3:
    try: t_env = int(os.getenv("SCORE_ALERT_THRESHOLD", "50"))
    except Exception: t_env = 50
    kpi("Críticas (< limiar env)", int((df["score"] < t_env).sum()))
with c4: kpi("Tokens distintos", df["token"].nunique() if "token" in df.columns else 0)
st.markdown(f"""<div class="explain">💡 KPIs resumem a visão atual do recorte.</div>""", unsafe_allow_html=True)

# Parâmetros & Ações
st.markdown('<div class="section-title">Parâmetros & Ações</div>', unsafe_allow_html=True)
ac1, ac2, ac3 = st.columns([2, 2, 2])
with ac1:
    alert_input = st.number_input("Limiar de alerta (score < x)", min_value=0, max_value=100, value=int(os.getenv("SCORE_ALERT_THRESHOLD", "50")))
with ac2:
    use_filters = st.checkbox("Gerar PDF usando filtros atuais", value=True)
with ac3:
    # >>> chave única diferente do botão superior
    if st.button("⚡ Coletar agora (ETH)", key="collect_actions"):
        with st.spinner("Coletando on-chain e gerando CSVs..."):
            n = collect_and_score_now()
        if n > 0:
            st.success(f"Coleta concluída ({n} transações). Recarregando…")
            st.rerun()
        else:
            st.error("Não foi possível coletar dados agora. Confira as secrets (ETH_RPC_URL etc.).")
st.markdown(f"""<div class="explain">💡 Clique em <b>Coletar agora (ETH)</b> para popular rapidamente o painel neste container efêmero.</div>""", unsafe_allow_html=True)

# Gráfico
st.markdown('<div class="section-title">Distribuição por token</div>', unsafe_allow_html=True)
bar_chart_token(df)
st.markdown(f"""<div class="explain">💡 Contagem por token no recorte selecionado.</div>""", unsafe_allow_html=True)

# Tabela
st.markdown('<div class="section-title">Transações</div>', unsafe_allow_html=True)
cols_show = ["tx_id","timestamp","from_address","to_address","amount","token","method","chain","score","penalty_total"]
cols_show = [c for c in cols_show if c in df.columns]
st.dataframe(df[cols_show].sort_values(by="timestamp", ascending=False), use_container_width=True, height=420)
st.markdown(f"""<div class="explain">💡 Ordene por score para priorizar investigações.</div>""", unsafe_allow_html=True)

# Contribuição por regra
if show_contrib:
    st.markdown('<div class="section-title">Contribuição por regra (%)</div>', unsafe_allow_html=True)
    ctab = contrib_table(df)
    if ctab.empty:
        st.caption("Sem dados de contribuição disponíveis neste arquivo.")
    else:
        st.dataframe(ctab.sort_values(by=["tx_id","pct"], ascending=[True, False]), use_container_width=True, height=360)
    st.markdown(f"""<div class="explain">💡 Percentual de participação de cada regra na penalidade total.</div>""", unsafe_allow_html=True)

# PDF
st.markdown('<div class="section-title">Relatório (PDF)</div>', unsafe_allow_html=True)
# >>> chave única para o botão de PDF
if st.button("🧾 Gerar PDF de críticos", key="pdf_button"):
    base_df = df if use_filters else (df_all[df_all["chain"] == chain] if "chain" in df_all.columns else df_all)
    crit = base_df[base_df["score"] < alert_input].copy()
    try:
        pdf_bytes = generate_pdf_bytes(crit, alert_input, f"{fname} | chain={chain} | filtros={'on' if use_filters else 'off'}")
        st.success("PDF gerado com sucesso! Use o botão para baixar.", icon="✅")
        st.download_button("⬇️ Baixar PDF", data=pdf_bytes, file_name="relatorio_criticos.pdf", mime="application/pdf", use_container_width=True, key="pdf_download")
    except Exception as e:
        st.error(f"Falha ao gerar PDF: {e}", icon="⚠️")
st.markdown(f"""<div class="explain">💡 O PDF lista transações com score abaixo do limiar.</div>""", unsafe_allow_html=True)
