import os, json, time
from pathlib import Path

import numpy as np
import pandas as pd
import re
from pytrends.request import TrendReq

def extract_etl_source2(raw_sales_path: Path) -> Path:
    keywords = ["coffee", "bakery", "tea", "chocolate"]
    geo = "US-NY"

    timeframe = derive_timeframe_from_sales(raw_sales_path)

    out_path = RAW_DIR / "source2_pytrends_interest_over_time_raw.csv"

    t0 = time.time()
    pytrends = TrendReq(hl="en-US", tz=420)
    pytrends.build_payload(keywords, timeframe=timeframe, geo=geo)
    iot = pytrends.interest_over_time()   # RAW

    iot.to_csv(out_path, index=True)      # RAW save (index date)
    exec_s = time.time() - t0

    append_jsonl(EXTRACT_LOG, {
        "stage": "extract",
        "source_name": "google_trends_pytrends_interest_over_time",
        "params": {"keywords": keywords, "geo": geo, "timeframe": timeframe},
        "output_file": str(out_path),
        "rows": int(iot.shape[0]),
        "cols": int(iot.shape[1]),
        "size_bytes": int(out_path.stat().st_size),
        "exec_seconds": round(exec_s, 4),
    })
    return out_path
