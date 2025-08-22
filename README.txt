# SafeScore

Antifraude com score 0–100 para transações on-chain ou mock. Gera CSV, alerta (Telegram) e dashboard em Streamlit.

## Rodar local
```bash
python -m venv .venv && . .venv/Scripts/activate  # (Windows) ou source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # opcional

# Coleta + score + CSV
python main.py

# Dashboard
streamlit run app.py
