"""
Calculo de indicadores tecnicos usando la libreria 'ta'.
Recibe un DataFrame con OHLCV y devuelve otro DataFrame con los indicadores.
"""
import logging
import pandas as pd
from ta.trend import SMAIndicator, EMAIndicator, MACD
from ta.momentum import RSIIndicator
from ta.volatility import BollingerBands

logger = logging.getLogger(__name__)


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula indicadores tecnicos para UN ticker.

    Args:
        df: DataFrame con columnas ['ticker', 'date', 'close']
            ordenado cronologicamente (mas antiguo primero).

    Returns:
        DataFrame con columnas:
        ticker, date, sma_20, sma_50, ema_20,
        rsi_14, macd, macd_signal, macd_hist,
        bollinger_upper, bollinger_mid, bollinger_lower
    """
    if df.empty:
        return pd.DataFrame()

    if not df["date"].is_monotonic_increasing:
        df = df.sort_values("date").reset_index(drop=True)

    close = df["close"].astype(float)

    # --- Tendencia ---
    sma_20 = SMAIndicator(close=close, window=20).sma_indicator()
    sma_50 = SMAIndicator(close=close, window=50).sma_indicator()
    ema_20 = EMAIndicator(close=close, window=20).ema_indicator()

    # --- Momentum ---
    rsi_14 = RSIIndicator(close=close, window=14).rsi()

    # --- MACD (12, 26, 9) ---
    macd_obj = MACD(close=close, window_slow=26, window_fast=12, window_sign=9)
    macd_line = macd_obj.macd()
    macd_sig = macd_obj.macd_signal()
    macd_hist = macd_obj.macd_diff()

    # --- Bollinger Bands (20 dias, 2 desviaciones) ---
    bb = BollingerBands(close=close, window=20, window_dev=2)
    bb_upper = bb.bollinger_hband()
    bb_mid = bb.bollinger_mavg()
    bb_lower = bb.bollinger_lband()

    out = pd.DataFrame({
        "ticker": df["ticker"].values,
        "date": df["date"].values,
        "sma_20": sma_20.values,
        "sma_50": sma_50.values,
        "ema_20": ema_20.values,
        "rsi_14": rsi_14.values,
        "macd": macd_line.values,
        "macd_signal": macd_sig.values,
        "macd_hist": macd_hist.values,
        "bollinger_upper": bb_upper.values,
        "bollinger_mid": bb_mid.values,
        "bollinger_lower": bb_lower.values,
    })

    logger.info(
        f"  Indicadores calculados: {len(out)} filas "
        f"({out['rsi_14'].notna().sum()} con RSI valido)"
    )
    return out


def compute_for_many(df_all: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula indicadores para varios tickers a la vez.
    Agrupa por ticker para que los calculos no se mezclen entre acciones.
    """
    if df_all.empty:
        return pd.DataFrame()

    frames = []
    for ticker, group in df_all.groupby("ticker", sort=False):
        logger.info(f"Calculando indicadores para {ticker} ({len(group)} filas)...")
        ind = compute_indicators(group)
        if not ind.empty:
            frames.append(ind)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)