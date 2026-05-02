"""Pipeline ETL principal."""
import logging
import os
import sys

from app.etl.extract import fetch_many
from app.etl.load import upsert_stock_prices

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("logs/etl.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("main")

DEFAULT_TICKERS = ["AAPL", "TSLA", "NVDA", "AMZN", "MSFT", "GOOGL"]
DEFAULT_PERIOD = "1y"


def resolve_params() -> tuple[list[str], str]:
    if len(sys.argv) > 1:
        return [t.upper() for t in sys.argv[1:]], DEFAULT_PERIOD

    tickers_env = os.getenv("TICKERS_INPUT", "").strip()
    period_env = os.getenv("PERIOD_INPUT", "").strip()

    tickers = tickers_env.split() if tickers_env else DEFAULT_TICKERS
    period = period_env if period_env else DEFAULT_PERIOD

    return [t.upper() for t in tickers], period


def run(tickers: list[str], period: str) -> int:
    log.info(f"=== ETL inicio | tickers={tickers} | period={period} ===")

    df = fetch_many(tickers, period=period)
    log.info(f"Total filas extraidas: {len(df)}")

    if df.empty:
        log.error("No se obtuvieron datos. Abortando carga.")
        return 1

    n = upsert_stock_prices(df)
    log.info(f"=== ETL fin | filas procesadas: {n} ===")
    return 0


if __name__ == "__main__":
    tickers, period = resolve_params()
    sys.exit(run(tickers, period))