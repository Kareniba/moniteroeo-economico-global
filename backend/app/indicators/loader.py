"""
Lectura de stock_prices desde Supabase y carga de indicadores tecnicos.
"""
import logging
import pandas as pd
from app.database.supabase_client import get_supabase

logger = logging.getLogger(__name__)

PAGE_SIZE = 1000     # Supabase devuelve max 1000 filas por defecto
BATCH_SIZE = 500     # Tamaño de lote para upsert


def fetch_prices(tickers: list[str] | None = None) -> pd.DataFrame:
    """
    Lee stock_prices desde Supabase. Si tickers=None lee todos.
    Pagina automaticamente si hay mas de 1000 filas.
    """
    sb = get_supabase()

    all_rows = []
    offset = 0
    while True:
        query = (
            sb.table("stock_prices")
            .select("ticker, date, close")
            .order("ticker")
            .order("date")
            .range(offset, offset + PAGE_SIZE - 1)
        )
        if tickers:
            query = query.in_("ticker", tickers)

        resp = query.execute()
        rows = resp.data or []
        if not rows:
            break

        all_rows.extend(rows)
        if len(rows) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    df = pd.DataFrame(all_rows)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["close"] = df["close"].astype(float)

    logger.info(f"Leidas {len(df)} filas de stock_prices")
    return df


def upsert_indicators(df: pd.DataFrame) -> int:
    """Inserta o actualiza filas en technical_indicators."""
    if df.empty:
        logger.warning("DataFrame vacio, no se carga nada")
        return 0

    sb = get_supabase()

    records = df.copy()
    records["date"] = records["date"].astype(str)

    numeric_cols = [
        "sma_20", "sma_50", "ema_20",
        "rsi_14", "macd", "macd_signal", "macd_hist",
        "bollinger_upper", "bollinger_mid", "bollinger_lower",
    ]
    for col in numeric_cols:
        if col in records.columns:
            records[col] = pd.to_numeric(records[col], errors="coerce")

    payload = records.to_dict(orient="records")
    payload = [{k: (None if pd.isna(v) else v) for k, v in r.items()} for r in payload]

    total = 0
    for i in range(0, len(payload), BATCH_SIZE):
        chunk = payload[i:i + BATCH_SIZE]
        sb.table("technical_indicators").upsert(
            chunk,
            on_conflict="ticker,date",
        ).execute()
        total += len(chunk)
        logger.info(f"  Cargado lote {i // BATCH_SIZE + 1}: {len(chunk)} filas")

    return total