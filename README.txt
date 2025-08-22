# SafeScore (Challenge FIAP × TecBan)

Solução antifraude que calcula **score de risco (0–100)** para transações (mock e/ou on-chain), gera **alertas**, **CSV** e **PDF**, e exibe um **dashboard** no Streamlit.

> **Conceito de score:** começa em **100** e **perde pontos** conforme regras de risco disparam. **Quanto menor o score, mais arriscada é a transação.**  
> **Alerta/Prevenção:** se `score < SCORE_ALERT_THRESHOLD`, a transação é **crítica** (alerta Telegram e fila de retenção `pending_review.csv`).

## Estrutura
