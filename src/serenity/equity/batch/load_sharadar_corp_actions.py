import datetime

import luigi

from serenity.equity.batch.utils import LoadSharadarTableTask, ExportQuandlTableTask
from serenity.equity.sharadar_api import clean_nulls
from serenity.equity.sharadar_refdata import Ticker, CorporateActionType, CorporateAction


class LoadCorporateActionsTask(LoadSharadarTableTask):
    start_date = luigi.DateParameter(default=datetime.date.today())
    end_date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        yield ExportQuandlTableTask(table_name='SHARADAR/ACTIONS', date_column='date',
                                    start_date=self.start_date, end_date=self.end_date)

    def process_row(self, index, row):
        ticker_code = clean_nulls(row['ticker'])
        ticker = Ticker.find_by_ticker(self.session, ticker_code)
        if ticker is None:
            self.logger.warning(f'unknown ticker referenced; skipping: {ticker_code}')
            return

        corp_action_date = row['date']
        corp_action_type_code = row['action']
        corp_action_type = CorporateActionType.get_or_create(self.session, corp_action_type_code)

        name = row['name']
        value = clean_nulls(row['value'])
        contra_ticker = clean_nulls(row['contraticker'])
        contra_name = clean_nulls(row['contraname'])

        corp_action = CorporateAction.find(self.session, ticker_code, corp_action_date, corp_action_type_code)
        if corp_action is None:
            corp_action = CorporateAction(corp_action_date=corp_action_date, ticker=ticker,
                                          corp_action_type=corp_action_type, name=name,
                                          value=value, contra_ticker=contra_ticker, contra_name=contra_name)
        else:
            corp_action.name = name
            corp_action.value = value
            corp_action.contra_ticker = contra_ticker
            corp_action.contra_name = contra_name

        self.session.add(corp_action)
