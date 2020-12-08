import hashlib
import logging
from abc import ABC, abstractmethod

import luigi
import pandas as pd
import quandl
from luigi.contrib.simulate import RunAnywayTarget

from serenity.equity.sharadar_api import create_sharadar_session


class QuandlApi(luigi.Config):
    api_key = luigi.Parameter()


class ExportQuandlTableTask(luigi.Task):
    logger = logging.getLogger('luigi-interface')

    table_name = luigi.Parameter()
    date_column = luigi.Parameter(default='')
    start_date = luigi.DateParameter(default=None)
    end_date = luigi.DateParameter(default=None)
    api_key = luigi.configuration.get_config().get('QuandlApi', 'api_key', None)

    def output(self):
        if self.start_date is None and self.end_date is None:
            return luigi.LocalTarget(f'data/{str(self.table_name).replace("/", "-")}_quandl_data.zip')
        else:
            end_date_txt = str(self.end_date)
            if self.end_date is None:
                end_date_txt = 'LATEST'
            date_range = f'{self.start_date}-{end_date_txt}'
            return luigi.LocalTarget(f'data/{str(self.table_name).replace("/", "-")}_{date_range}_quandl_data.zip')

    def run(self):
        quandl.ApiConfig.api_key = str(self.api_key)
        with self.output().temporary_path() as path:
            self.logger.info(f'downloading {self.table_name} to {path}')
            if self.date_column == '':
                kwargs = {
                    'filename': path
                }
            else:
                kwargs = {
                    'filename': path,
                    str(self.date_column): {'gte': self.start_date, 'lte': self.end_date}
                }
            quandl.export_table(str(self.table_name), **kwargs)


class LoadSharadarTableTask(ABC, luigi.Task):
    logger = logging.getLogger('luigi-interface')
    session = create_sharadar_session()

    def run(self):
        for table_in in self.input():
            in_file = table_in.path
            df = pd.read_csv(in_file)
            self.logger.info(f'loaded {len(df)} rows of CSV data from {in_file}')

            md5 = hashlib.md5(open(in_file, 'rb').read()).hexdigest()
            self.logger.info(f'computed MD5 checksum for {in_file}: {md5}')

            row_count = 0
            for index, row in df.iterrows():
                self.process_row(index, row)
                if row_count > 0 and row_count % 1000 == 0:
                    self.logger.info(f'{row_count} rows loaded; flushing next 1000 rows to database')
                    self.session.commit()
                row_count += 1

            self.session.commit()

        # mark complete
        self.output().done()

    def output(self):
        return RunAnywayTarget(self)

    @abstractmethod
    def process_row(self, index, row):
        pass
