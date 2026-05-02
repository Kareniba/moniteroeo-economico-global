"""
Pipeline de calculo de indicadores tecnicos.

Uso:
    python compute_indicators.py              # todos los tickers
    python compute_indicators.py AAPL         # solo AAPL (debug)
    python compute_indicators.py AAPL TSLA    # varios tickers
"""
import logging
import sys

from app.indicators.calculator import compute_for_many
from app.indicators.loader import fetch_prices, upsert_indicators

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("logs/indicators.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("compute_indicators")


def run(tickers: list[str] | None = None) -> int:
    log.info(f"=== Indicadores inicio | tickers={tickers or 'TODOS'} ===")

    prices = fetch_prices(tickers)
    if prices.empty:
        log.error("No hay precios en stock_prices. Ejecuta main.py primero.")
        return 1

    indicators = compute_for_many(prices)
    log.info(f"Total filas de indicadores generadas: {len(indicators)}")

    n = upsert_indicators(indicators)
    log.info(f"=== Indicadores fin | filas procesadas: {n} ===")
    return 0


if __name__ == "__main__":
    args = [t.upper() for t in sys.argv[1:]] if len(sys.argv) > 1 else None
    sys.exit(run(args))