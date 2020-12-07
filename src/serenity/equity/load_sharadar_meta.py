import logging

import fire
import quandl

from serenity.equity.sharadar_api import init_quandl, create_sharadar_session, yes_no_to_bool
from serenity.equity.sharadar_refdata import UnitType, Indicator
from serenity.utils import init_logging


logger = logging.getLogger(__name__)


def load_indicators():
    session = create_sharadar_session()
    table = 'SHARADAR/INDICATORS'
    logger.info(f'downloading {table} from Quandl API')
    df = quandl.get_table(table, paginate=True)
    logger.info(f'loaded {len(df)} rows of metadata from {table}')

    row_count = 0
    for index, row in df.iterrows():
        table_name, indicator, is_filter, is_primary_key, title, description, unit_type_code = row
        is_filter = yes_no_to_bool(is_filter)
        is_primary_key = yes_no_to_bool(is_primary_key)

        unit_type = UnitType.get_or_create(session, unit_type_code)

        ind_entity = Indicator.find_by_name(session, table_name, indicator)
        if ind_entity is None:
            ind_entity = Indicator(table_name=table_name,
                                   indicator=indicator,
                                   is_filter=is_filter,
                                   is_primary_key=is_primary_key,
                                   title=title,
                                   description=description,
                                   unit_type=unit_type)
            session.add(ind_entity)

        if row_count > 0 and row_count % 1000 == 0:
            logger.info(f'{row_count} rows loaded; flushing next 1000 rows to database')
            session.commit()
        row_count += 1

    session.commit()


if __name__ == '__main__':
    init_logging()
    init_quandl()
    fire.Fire(load_indicators)
