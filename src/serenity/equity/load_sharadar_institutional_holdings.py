import logging

import fire
import pandas as pd
import quandl

from serenity.equity.sharadar_api import init_quandl, create_sharadar_session, clean_nulls
from serenity.equity.sharadar_holdings import InstitutionalInvestor, SecurityType, InstitutionalHoldings
from serenity.equity.sharadar_refdata import Ticker
from serenity.utils import init_logging


logger = logging.getLogger(__name__)


def load_sharadar_institutional_holdings():
    init_quandl()
    session = create_sharadar_session()

    load_path = 'sharadar_institutional_holdings.zip'
    logger.info(f'downloading institutional holdings data to {load_path}')
    quandl.export_table('SHARADAR/SF3', filename=load_path)
    df = pd.read_csv(load_path)
    logger.info(f'loaded {len(df)} rows of institutional holdings CSV data from {load_path}')

    row_count = 0
    for index, row in df.iterrows():
        ticker_code = row['ticker']
        ticker = Ticker.find_by_ticker(session, ticker_code)
        if ticker is None:
            logger.warning(f'unknown ticker referenced; skipping: {ticker_code}')
            continue

        investor_name = row['investorname']
        investor = InstitutionalInvestor.get_or_create(session, investor_name)

        security_type_code = row['securitytype']
        security_type = SecurityType.get_or_create(session, security_type_code)

        calendar_date = row['calendardate']
        value = row['value']
        units = row['units']
        price = clean_nulls(row['price'])

        holdings = InstitutionalHoldings.find_holdings(session, ticker, investor, security_type, calendar_date)
        if holdings is None:
            holdings = InstitutionalHoldings(ticker=ticker, investor=investor, security_type=security_type,
                                             calendar_date=calendar_date, value=value, units=units, price=price)
        else:
            holdings.value = value
            holdings.units = units
            holdings.price = price

        session.add(holdings)

        if row_count > 0 and row_count % 1000 == 0:
            logger.info(f'{row_count} rows loaded; flushing next 1000 rows to database')
            session.commit()
        row_count += 1

    session.commit()


if __name__ == '__main__':
    init_logging()
    init_quandl()
    fire.Fire(load_sharadar_institutional_holdings)
