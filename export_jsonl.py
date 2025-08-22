import csv
import json
from pathlib import Path
from typing import List, Dict, Any

DATA_DIR = Path("app/data")
TX_CSV = DATA_DIR / "transactions.csv"
OUT_JSONL = DATA_DIR / "transactions.jsonl"

def read_rows() -> List[Dict[str, Any]]:
    if not TX_CSV.exists():
        return []
    with TX_CSV.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def main():
    rows = read_rows()
    with OUT_JSONL.open("w", encoding="utf-8") as f:
        for r in rows:
            try:
                r["explain"] = json.loads(r.get("explain","{}"))
            except Exception:
                r["explain"] = {}
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"[OK] Exportado JSONL: {OUT_JSONL}")

if __name__ == "__main__":
    main()
