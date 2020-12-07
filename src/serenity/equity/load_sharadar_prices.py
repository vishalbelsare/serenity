import logging

import fire
import pandas as pd
import quandl

from serenity.equity.sharadar_api import init_quandl, create_sharadar_session, clean_nulls
from serenity.equity.sharadar_prices import EquityPrice
from serenity.equity.sharadar_refdata import Ticker
from serenity.utils import init_logging


logger = logging.getLogger(__name__)


def backfill_sharadar_prices():
    session = create_sharadar_session()

    load_path = 'sharadar_prices.zip'
    logger.info(f'downloading price data to {load_path}')
    quandl.export_table('SHARADAR/SEP', filename=load_path)
    df = pd.read_csv(load_path)
    logger.info(f'loaded {len(df)} rows of prices from {load_path}')

    row_count = 0
    for index, row in df.iterrows():
        ticker_code = row['ticker']
        date = row['date']
        open_px = row['open']
        high_px = row['high']
        low_px = row['low']
        close_px = row['close']
        volume = clean_nulls(row['volume'])
        dividends = row['dividends']
        close_unadj = row['closeunadj']
        last_updated = row['lastupdated']

        ticker = Ticker.find_by_ticker(session, ticker_code)
        if ticker is None:
            logger.warning(f'unknown ticker referenced; skipping: {ticker_code}')
            continue

        equity_price = EquityPrice(ticker=ticker, date=date, open_px=open_px, high_px=high_px, low_px=low_px,
                                   close_px=close_px, volume=volume, dividends=dividends, close_unadj=close_unadj,
                                   last_updated=last_updated)
        session.add(equity_price)

        if row_count > 0 and row_count % 1000 == 0:
            logger.info(f'{row_count} rows loaded; flushing next 1000 rows to database')
            session.commit()
        row_count += 1

    session.commit()


if __name__ == '__main__':
    init_logging()
    init_quandl()
    fire.Fire(backfill_sharadar_prices)
