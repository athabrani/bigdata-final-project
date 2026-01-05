import numpy as np
import pandas as pd

def validate_and_fix_data_quality(merged: pd.DataFrame, pk: str):
    dq_results = []

    unique_ok = merged[pk].is_unique
    dq_results.append({"rule": "uniqueness_check", "target": pk, "ok": bool(unique_ok)})
    if not unique_ok:
        merged = merged.drop_duplicates(subset=[pk], keep="first")

    critical_cols = [pk, "date_key", "gross_revenue"]
    null_ok = not merged[critical_cols].isnull().any().any()
    dq_results.append({"rule": "null_check", "target": critical_cols, "ok": bool(null_ok)})
    merged["gross_revenue"] = merged["gross_revenue"].fillna(0.0)
    merged["date_key"] = merged["date_key"].fillna(0).astype("int64")

    range_ok = (merged["gross_revenue"] >= 0).all()
    dq_results.append({"rule": "range_check", "target": "gross_revenue>=0", "ok": bool(range_ok)})
    merged.loc[merged["gross_revenue"] < 0, "gross_revenue"] = 0.0

    return merged, dq_results
