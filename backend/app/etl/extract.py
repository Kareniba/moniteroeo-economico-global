"""Extraccion de datos OHLCV desde Yahoo Finance."""
import logging
import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)


def fetch_ohlcv(ticker: str, period: str = "1y") -> pd.DataFrame:
    logger.info(f"Descargando {ticker} (period={period})...")
    df = yf.download(
        ticker,
        period=period,
        auto_adjust=False,
        progress=False,
        threads=False,
    )

    if df is None or df.empty:
        logger.warning(f"Sin datos para {ticker}")
        return pd.DataFrame()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df.reset_index()
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    df["ticker"] = ticker
    df["date"] = pd.to_datetime(df["date"]).dt.date

    cols = ["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]
    cols = [c for c in cols if c in df.columns]
    df = df[cols].dropna(subset=["close"])

    logger.info(f"  {ticker}: {len(df)} filas obtenidas")
    return df


def fetch_many(tickers: list[str], period: str = "1y") -> pd.DataFrame:
    frames = []
    for t in tickers:
        df = fetch_ohlcv(t, period=period)
        if not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)
