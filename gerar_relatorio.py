import os
import csv
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

from fpdf import FPDF  # type: ignore

# Caminhos
DATA_DIR = Path("app/data")
TX_CSV = DATA_DIR / "transactions.csv"
PDF_OUT = DATA_DIR / "relatorio.pdf"


# -------- utilidades --------
def ensure_data_dir() -> None:
    """Garante que a pasta app/data exista."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def load_threshold() -> int:
    """Lê SCORE_ALERT_THRESHOLD do ambiente (.env é carregado pelo main.py; aqui usamos direto o os.getenv)."""
    try:
        return int(os.getenv("SCORE_ALERT_THRESHOLD", "50"))
    except Exception:
        return 50


def read_transactions() -> List[Dict[str, Any]]:
    """Lê o CSV de transações. Retorna [] se não existir."""
    if not TX_CSV.exists():
        return []
    with TX_CSV.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def safe_text(text: str) -> str:
    """
    Converte texto para algo compatível com FPDF 1.x (Latin-1).
    Substitui travessão/reticências e remove caracteres fora da página de código.
    """
    if text is None:
        return ""
    t = str(text)
    t = t.replace("—", "-").replace("–", "-").replace("…", "...")
    # força latin-1; substitui caracteres fora do range por '?'
    return t.encode("latin-1", "replace").decode("latin-1")


# -------- PDF --------
class ReportPDF(FPDF):
    def header(self):
        # Evitar caracteres fora do latin-1 no título
        self.set_font("Arial", "B", 14)
        self.cell(0, 10, safe_text("Relatorio SafeScore - Transacoes Criticas"), ln=True, align="C")
        self.ln(3)


def build_pdf(rows: List[Dict[str, Any]], threshold: int) -> Path:
    pdf = ReportPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    # Cabeçalho informativo
    pdf.set_font("Arial", "", 11)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total = len(rows)
    pdf.multi_cell(0, 6, safe_text(f"Data de geracao: {now}"))
    pdf.multi_cell(0, 6, safe_text(f"Limiar de alerta: score < {threshold}"))
    pdf.multi_cell(0, 6, safe_text(f"Total de criticos: {total}"))
    pdf.ln(4)

    # Corpo
    if total == 0:
        pdf.set_font("Arial", "I", 11)
        pdf.multi_cell(0, 6, safe_text("Nenhuma transacao critica encontrada no periodo."))
    else:
        # Cabeçalho da tabela
        pdf.set_font("Arial", "B", 11)
        pdf.cell(35, 8, safe_text("TX"), border=1)
        pdf.cell(30, 8, safe_text("Score"), border=1)
        pdf.cell(40, 8, safe_text("From"), border=1)
        pdf.cell(40, 8, safe_text("To"), border=1)
        pdf.cell(35, 8, safe_text("Valor"), border=1, ln=True)

        # Linhas
        pdf.set_font("Arial", "", 10)
        for r in rows:
            tx = safe_text(str(r.get("tx_id", ""))[:18])
            sc = safe_text(str(r.get("score", "")))
            fr_raw = safe_text(str(r.get("from_address", "")))
            to_raw = safe_text(str(r.get("to_address", "")))
            fr = fr_raw[:18] + ("..." if len(fr_raw) > 18 else "")
            to = to_raw[:18] + ("..." if len(to_raw) > 18 else "")
            val = safe_text(f"{r.get('amount', '')} {r.get('token', '')}")[:18]

            pdf.cell(35, 7, tx, border=1)
            pdf.cell(30, 7, sc, border=1)
            pdf.cell(40, 7, fr, border=1)
            pdf.cell(40, 7, to, border=1)
            pdf.cell(35, 7, val, border=1, ln=True)

        # Motivos
        pdf.ln(4)
        pdf.set_font("Arial", "B", 11)
        pdf.cell(0, 8, safe_text("Motivos (top 10 por criticidade):"), ln=True)
        pdf.set_font("Arial", "", 10)

        top = sorted(rows, key=lambda x: int(x.get("score", 100)))[:10]
        for r in top:
            txid = safe_text(r.get("tx_id", ""))
            reasons = safe_text(r.get("reasons", "") or "n/d")
            pdf.multi_cell(0, 6, safe_text(f"- {txid} - {reasons}"))

    # Escrita do arquivo (fallback se arquivo estiver aberto no Windows)
    try:
        pdf.output(str(PDF_OUT))
        return PDF_OUT
    except PermissionError:
        alt = DATA_DIR / f"relatorio_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        pdf.output(str(alt))
        return alt


def main() -> None:
    ensure_data_dir()
    threshold = load_threshold()
    all_rows = read_transactions()
    # críticos = score < threshold
    critical = [r for r in all_rows if int(str(r.get("score", "0"))) < threshold]
    path = build_pdf(critical, threshold)
    print(f"[OK] Relatorio gerado: {path}")


if __name__ == "__main__":
    main()
