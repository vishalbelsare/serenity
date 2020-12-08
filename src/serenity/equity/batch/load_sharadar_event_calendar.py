import datetime

import luigi

from serenity.equity.batch.utils import LoadSharadarTableTask, ExportQuandlTableTask
from serenity.equity.sharadar_refdata import Ticker, get_indicator_details, EventCode, Event


class LoadEventCalendarTask(LoadSharadarTableTask):
    start_date = luigi.DateParameter(default=datetime.date.today())
    end_date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        yield ExportQuandlTableTask(table_name='SHARADAR/EVENTS', date_column='date',
                                    start_date=self.start_date, end_date=self.end_date)

    def process_row(self, index, row):
        ticker_code = row['ticker']
        ticker = Ticker.find_by_ticker(self.session, ticker_code)
        if ticker is None:
            self.logger.warning(f'unknown ticker referenced; skipping: {ticker_code}')
            return

        event_date = row['date']
        event_codes = row['eventcodes']
        for event_code in event_codes.split('|'):
            indicator = get_indicator_details(self.session, 'EVENTCODES', event_code)
            event_code_entity = EventCode.get_or_create(self.session, int(event_code), indicator.title)
            event = Event.find(self.session, ticker.ticker, event_date, int(event_code))
            if event is None:
                event = Event(ticker=ticker, event_date=event_date, event_code=event_code_entity)
                self.session.add(event)
