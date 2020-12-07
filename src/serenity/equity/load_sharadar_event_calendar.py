import fire
import pandas as pd
import quandl

from serenity.equity.sharadar_api import init_quandl, create_sharadar_session
from serenity.equity.sharadar_refdata import Ticker, get_indicator_details, EventCode, Event


def load_sharadar_events():
    init_quandl()
    session = create_sharadar_session()
    load_path = 'sharadar_events.zip'
    quandl.export_table('SHARADAR/EVENTS', filename=load_path)
    df = pd.read_csv(load_path)
    row_count = 0
    for index, row in df.iterrows():
        ticker_code = row['ticker']
        ticker = Ticker.find_by_ticker(session, ticker_code)
        if ticker is None:
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

        if row_count % 1000 == 0:
            session.commit()
        row_count += 1

    session.commit()


if __name__ == '__main__':
    fire.Fire(load_sharadar_events)
