"""Carga datos OHLCV en Supabase usando upsert."""
import logging
import pandas as pd
from app.database.supabase_client import get_supabase

logger = logging.getLogger(__name__)

BATCH_SIZE = 500


def upsert_stock_prices(df: pd.DataFrame) -> int:
    if df.empty:
        logger.warning("DataFrame vacio, no se carga nada")
        return 0

    sb = get_supabase()

    records = df.copy()
    records["date"] = records["date"].astype(str)

    for col in ["open", "high", "low", "close", "adj_close"]:
        if col in records.columns:
            records[col] = records[col].astype(float)

    if "volume" in records.columns:
        records["volume"] = records["volume"].astype("Int64").astype(object)

    payload = records.to_dict(orient="records")
    payload = [{k: (None if pd.isna(v) else v) for k, v in r.items()} for r in payload]

    total = 0
    for i in range(0, len(payload), BATCH_SIZE):
        chunk = payload[i:i + BATCH_SIZE]
        sb.table("stock_prices").upsert(
            chunk,
            on_conflict="ticker,date",
        ).execute()
        total += len(chunk)
        logger.info(f"  Cargado lote {i // BATCH_SIZE + 1}: {len(chunk)} filas")

    return total
