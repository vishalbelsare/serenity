import datetime

import luigi

from serenity.equity.batch.load_sharadar_tickers import LoadSharadarTickersTask
from serenity.equity.batch.utils import LoadSharadarTableTask, ExportQuandlTableTask
from serenity.equity.sharadar_api import clean_nulls
from serenity.equity.sharadar_prices import EquityPrice
from serenity.equity.sharadar_refdata import Ticker


class LoadEquityPricesTask(LoadSharadarTableTask):
    start_date = luigi.DateParameter(default=datetime.date.today())
    end_date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        yield LoadSharadarTickersTask(start_date=self.start_date, end_date=self.end_date)
        yield ExportQuandlTableTask(table_name=self.get_workflow_name(), date_column='lastupdated',
                                    start_date=self.start_date, end_date=self.end_date)

    def process_row(self, index, row):
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

        ticker = Ticker.find_by_ticker(self.session, ticker_code)
        if ticker is None:
            self.logger.warning(f'unknown ticker referenced; skipping: {ticker_code}')
            return

        equity_price = EquityPrice.find(self.session, ticker_code, date)
        if equity_price is None:
            equity_price = EquityPrice(ticker=ticker, date=date, open_px=open_px, high_px=high_px, low_px=low_px,
                                       close_px=close_px, volume=volume, dividends=dividends, close_unadj=close_unadj,
                                       last_updated=last_updated)
        else:
            equity_price.open_px = open_px
            equity_price.high_px = high_px
            equity_price.low_px = low_px
            equity_price.close_px = close_px
            equity_price.volume = volume
            equity_price.dividends = dividends
            equity_price.close_unadj = close_unadj
            equity_price.last_updated = last_updated

        self.session.add(equity_price)

    def get_workflow_name(self):
        return 'SHARADAR/SEP'
