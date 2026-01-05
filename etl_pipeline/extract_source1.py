import time, json
from pathlib import Path
import pandas as pd
from pytrends.request import TrendReq

BASE_DIR = Path("/content/bigdata_final_project")
RAW_DIR = BASE_DIR / "raw"
LOG_DIR = BASE_DIR / "logs" / "etl"

RAW_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

EXTRACT_LOG = LOG_DIR / "extract_log.jsonl"

def append_jsonl(path: Path, record: dict):
    record = dict(record)
    record["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

def extract_etl_source1() -> Path:
    url = "https://raw.githubusercontent.com/athabrani/Tugas-Besar-Big-Data-2025/main/raw/Coffee%20Shop%20Sales.csv"
    out_path = RAW_DIR / "source1_coffee_shop_sales_raw.csv"

    t0 = time.time()
    df = pd.read_csv(url)
    df.to_csv(out_path, index=False)
    exec_s = time.time() - t0

    append_jsonl(EXTRACT_LOG, {
        "stage": "extract",
        "source_name": "github_raw_csv_coffee_shop_sales",
        "output_file": str(out_path),
        "rows": int(df.shape[0]),
        "cols": int(df.shape[1]),
        "size_bytes": int(out_path.stat().st_size),
        "exec_seconds": round(exec_s, 4),
    })
    return out_path
