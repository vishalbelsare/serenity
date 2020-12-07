import datetime

import luigi

from serenity.equity.batch.load_sharadar_tickers import LoadSharadarTickersTask


class SharadarDailyDownloadTask(luigi.WrapperTask):
    start_date = luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(14))
    end_date = luigi.DateParameter(default=None)

    def requires(self):
        yield LoadSharadarTickersTask(start_date=self.start_date, end_date=self.end_date)


if __name__ == '__main__':
    luigi.build([SharadarDailyDownloadTask()], local_scheduler=True)
