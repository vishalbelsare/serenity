import fire
import pandas as pd
import quandl

from serenity.equity.sharadar_api import init_quandl, create_sharadar_session, clean_nulls
from serenity.equity.sharadar_holdings import InstitutionalInvestor, SecurityType, InstitutionalHoldings
from serenity.equity.sharadar_refdata import Ticker


def load_sharadar_institutional_holdings():
    init_quandl()
    session = create_sharadar_session()

    load_path = 'sharadar_institutional_holdings.zip'
    quandl.export_table('SHARADAR/SF3', filename=load_path)
    df = pd.read_csv(load_path)
    for index, row in df.iterrows():
        ticker_code = row['ticker']
        ticker = Ticker.find_by_ticker(session, ticker_code)
        if ticker is None:
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

    session.commit()


if __name__ == '__main__':
    fire.Fire(load_sharadar_institutional_holdings)
