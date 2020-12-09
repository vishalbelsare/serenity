import datetime

import luigi

from serenity.equity.batch.load_sharadar_corp_actions import LoadCorporateActionsTask
from serenity.equity.batch.load_sharadar_event_calendar import LoadEventCalendarTask
from serenity.equity.batch.load_sharadar_fundamentals import LoadEquityFundamentalsTask
from serenity.equity.batch.load_sharadar_insider_holdings import LoadInsiderHoldingsTask
from serenity.equity.batch.load_sharadar_institutional_holdings import LoadInstitutionalHoldingsTask
from serenity.equity.batch.load_sharadar_meta import LoadSharadarMetaTask
from serenity.equity.batch.load_sharadar_prices import LoadEquityPricesTask


class SharadarDailyDownloadTask(luigi.WrapperTask):
    start_date = luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(days=8))
    end_date = luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(days=1))

    def requires(self):
        yield LoadSharadarMetaTask()
        yield LoadCorporateActionsTask(start_date=self.start_date, end_date=self.end_date)
        yield LoadEventCalendarTask(start_date=self.start_date, end_date=self.end_date)
        yield LoadEquityPricesTask(start_date=self.start_date, end_date=self.end_date)
        yield LoadEquityFundamentalsTask(start_date=self.start_date, end_date=self.end_date)
        yield LoadInsiderHoldingsTask(start_date=self.start_date, end_date=self.end_date)
        yield LoadInstitutionalHoldingsTask(start_date=self.start_date, end_date=self.end_date)


if __name__ == '__main__':
    luigi.build([SharadarDailyDownloadTask()], workers=1, local_scheduler=True)
