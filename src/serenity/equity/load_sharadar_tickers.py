import logging

import fire
import pandas as pd
import quandl

from serenity.equity.sharadar_api import init_quandl, create_sharadar_session, clean_nulls, yes_no_to_bool
from serenity.equity.sharadar_refdata import Exchange, TickerCategory, Sector, Scale, Currency, Ticker
from serenity.utils import init_logging


logger = logging.getLogger(__name__)


# noinspection DuplicatedCode
def load_tickers():
    session = create_sharadar_session()
    load_path = 'sharadar_tickers.zip'
    logger.info(f'downloading ticker data to {load_path}')
    quandl.export_table('SHARADAR/TICKERS', filename=load_path)
    df = pd.read_csv(load_path)
    logger.info(f'loaded {len(df)} rows of ticker CSV data from {load_path}')

    row_count = 0
    for index, row in df.iterrows():
        table_name = row['table']
        perma_ticker_id = row['permaticker']
        ticker = row['ticker']
        if ticker == 'N/A':
            ticker = None
        name = row['name']
        exchange = Exchange.get_or_create(session, clean_nulls(row['exchange']))
        is_delisted = yes_no_to_bool(row['isdelisted'])
        category = TickerCategory.get_or_create(session, clean_nulls(row['category']))

        cusips = row['cusips']

        sic_sector_code = clean_nulls(row['siccode'])
        sic_sector = clean_nulls(row['sicsector'])
        sic_industry = clean_nulls(row['sicindustry'])
        sic_sector_map = Sector.get_or_create(session, sector_code_type_code='SIC', sector_code=sic_sector_code,
                                              sector=sic_sector, industry=sic_industry)

        fama_sector = clean_nulls(row['famasector'])
        fama_industry = clean_nulls(row['famaindustry'])
        fama_sector_map = Sector.get_or_create(session, sector_code_type_code='FAMA', sector_code=None,
                                               sector=fama_sector, industry=fama_industry)

        sector = clean_nulls(row['sector'])
        industry = clean_nulls(row['industry'])
        sharadar_sector_map = Sector.get_or_create(session, sector_code_type_code='Sharadar', sector_code=None,
                                                   sector=sector, industry=industry)

        market_cap_scale_code = clean_nulls(row['scalemarketcap'])
        market_cap_scale = Scale.get_or_create(session, market_cap_scale_code)

        revenue_scale_code = clean_nulls(row['scalerevenue'])
        revenue_scale = Scale.get_or_create(session, revenue_scale_code)

        related_tickers = row['relatedtickers']

        currency_code = row['currency']
        currency = Currency.get_or_create(session, currency_code)

        location = row['location']
        last_updated = clean_nulls(row['lastupdated'])
        first_added = clean_nulls(row['firstadded'])
        first_price_date = clean_nulls(row['firstpricedate'])
        last_price_date = clean_nulls(row['lastpricedate'])
        first_quarter = clean_nulls(row['firstquarter'])
        last_quarter = clean_nulls(row['lastquarter'])
        sec_filings = row['secfilings']
        company_site = row['companysite']

        ticker_entity = Ticker.find_by_perma_id(session, perma_ticker_id)
        if ticker_entity is not None:
            ticker_entity.table_name = table_name
            ticker_entity.ticker = ticker
            ticker_entity.name = name
            ticker_entity.exchange = exchange
            ticker_entity.is_delisted = is_delisted
            ticker_entity.ticker_category = category
            ticker_entity.cusips = cusips
            ticker_entity.sic_sector = sic_sector_map
            ticker_entity.fama_sector = fama_sector_map
            ticker_entity.sector = sharadar_sector_map
            ticker_entity.market_cap_scale = market_cap_scale
            ticker_entity.revenue_scale = revenue_scale
            ticker_entity.related_tickers = related_tickers
            ticker_entity.currency = currency
            ticker_entity.location = location
            ticker_entity.last_updated = last_updated
            ticker_entity.first_added = first_added
            ticker_entity.first_price_date = first_price_date
            ticker_entity.last_price_date = last_price_date
            ticker_entity.first_quarter = first_quarter
            ticker_entity.last_quarter = last_quarter
            ticker_entity.secfilings = sec_filings
            ticker_entity.company_site = company_site
        else:
            # noinspection PyTypeChecker
            ticker_entity = Ticker(table_name=table_name, ticker=ticker, name=name, perma_ticker_id=perma_ticker_id,
                                   exchange=exchange, is_delisted=is_delisted, ticker_category=category, cusips=cusips,
                                   sic_sector=sic_sector_map, fama_sector=fama_sector_map, sector=sharadar_sector_map,
                                   market_cap_scale=market_cap_scale, revenue_scale=revenue_scale,
                                   related_tickers=related_tickers, currency=currency, location=location,
                                   last_updated=last_updated, first_added=first_added,
                                   first_price_date=first_price_date, last_price_date=last_price_date,
                                   first_quarter=first_quarter, last_quarter=last_quarter, secfilings=sec_filings,
                                   company_site=company_site)
        session.add(ticker_entity)

        if row_count > 0 and row_count % 1000 == 0:
            logger.info(f'{row_count} rows loaded; flushing next 1000 rows to database')
            session.commit()
        row_count += 1

    session.commit()


if __name__ == '__main__':
    init_logging()
    init_quandl()
    fire.Fire(load_tickers)
