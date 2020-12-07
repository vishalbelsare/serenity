import fire
import pandas as pd
import quandl

from serenity.equity.sharadar_api import init_quandl, create_sharadar_session, clean_nulls
from serenity.equity.sharadar_refdata import Ticker, CorporateActionType, CorporateAction


def load_sharadar_corp_actions():
    init_quandl()
    session = create_sharadar_session()
    load_path = 'sharadar_corp_actions.zip'
    quandl.export_table('SHARADAR/ACTIONS', filename=load_path)
    df = pd.read_csv(load_path)
    row_count = 0
    for index, row in df.iterrows():
        ticker_code = clean_nulls(row['ticker'])
        ticker = Ticker.find_by_ticker(session, ticker_code)
        if ticker is None:
            continue

        corp_action_date = row['date']
        corp_action_type_code = row['action']
        corp_action_type = CorporateActionType.get_or_create(session, corp_action_type_code)

        name = row['name']
        value = clean_nulls(row['value'])
        contra_ticker = clean_nulls(row['contraticker'])
        contra_name = clean_nulls(row['contraname'])

        corp_action = CorporateAction.find(session, ticker_code, corp_action_date, corp_action_type_code)
        if corp_action is None:
            corp_action = CorporateAction(corp_action_date=corp_action_date, ticker=ticker,
                                          corp_action_type=corp_action_type, name=name,
                                          value=value, contra_ticker=contra_ticker, contra_name=contra_name)
        else:
            corp_action.name = name
            corp_action.value = value
            corp_action.contra_ticker = contra_ticker
            corp_action.contra_name = contra_name

        session.add(corp_action)

        if row_count % 1000 == 0:
            session.commit()
        row_count += 1

    session.commit()


if __name__ == '__main__':
    fire.Fire(load_sharadar_corp_actions)
