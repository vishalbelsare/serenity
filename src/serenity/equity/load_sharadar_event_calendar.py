import logging

import fire
import pandas as pd
import quandl

from serenity.equity.sharadar_api import init_quandl, create_sharadar_session
from serenity.equity.sharadar_refdata import Ticker, get_indicator_details, EventCode, Event
from serenity.utils import init_logging

logger = logging.getLogger(__name__)


def load_sharadar_events():
    session = create_sharadar_session()
    load_path = 'sharadar_events.zip'
    logger.info(f'downloading event calendar data to {load_path}')
    quandl.export_table('SHARADAR/EVENTS', filename=load_path)
    df = pd.read_csv(load_path)
    logger.info(f'loaded {len(df)} rows of event calendar CSV data from {load_path}')

    row_count = 0
    for index, row in df.iterrows():
        ticker_code = row['ticker']
        ticker = Ticker.find_by_ticker(session, ticker_code)
        if ticker is None:
            logger.warning(f'unknown ticker referenced; skipping: {ticker_code}')
            continue

        event_date = row['date']
        event_codes = row['eventcodes']
        for event_code in event_codes.split('|'):
            indicator = get_indicator_details(session, 'EVENTCODES', event_code)
            event_code_entity = EventCode.get_or_create(session, int(event_code), indicator.title)
            event = Event.find(session, ticker.ticker, event_date, int(event_code))
            if event is None:
                event = Event(ticker=ticker, event_date=event_date, event_code=event_code_entity)
                session.add(event)

        if row_count > 0 and row_count % 1000 == 0:
            logger.info(f'{row_count} rows loaded; flushing next 1000 rows to database')
            session.commit()
        row_count += 1

    session.commit()


if __name__ == '__main__':
    init_logging()
    init_quandl()
    fire.Fire(load_sharadar_events)
