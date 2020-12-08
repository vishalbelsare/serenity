import datetime

import luigi

from serenity.equity.batch.utils import LoadSharadarTableTask, ExportQuandlTableTask
from serenity.equity.sharadar_api import clean_nulls
from serenity.equity.sharadar_holdings import InstitutionalInvestor, SecurityType, InstitutionalHoldings
from serenity.equity.sharadar_refdata import Ticker


class LoadInstitutionalHoldingsTask(LoadSharadarTableTask):
    start_date = luigi.DateParameter(default=datetime.date.today())
    end_date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        yield ExportQuandlTableTask(table_name='SHARADAR/SF3', date_column='calendardate',
                                    start_date=self.start_date, end_date=self.end_date)

    def process_row(self, index, row):
        ticker_code = row['ticker']
        ticker = Ticker.find_by_ticker(self.session, ticker_code)
        if ticker is None:
            self.logger.warning(f'unknown ticker referenced; skipping: {ticker_code}')
            return

        investor_name = row['investorname']
        investor = InstitutionalInvestor.get_or_create(self.session, investor_name)

        security_type_code = row['securitytype']
        security_type = SecurityType.get_or_create(self.session, security_type_code)

        calendar_date = row['calendardate']
        value = row['value']
        units = row['units']
        price = clean_nulls(row['price'])

        holdings = InstitutionalHoldings.find_holdings(self.session, ticker, investor, security_type, calendar_date)
        if holdings is None:
            holdings = InstitutionalHoldings(ticker=ticker, investor=investor, security_type=security_type,
                                             calendar_date=calendar_date, value=value, units=units, price=price)
        else:
            holdings.value = value
            holdings.units = units
            holdings.price = price

        self.session.add(holdings)
