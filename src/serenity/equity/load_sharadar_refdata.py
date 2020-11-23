import fire
import pandas as pd
import quandl
from sqlalchemy.orm import Session

from serenity.equity.sharadar_api import init_quandl, create_sharadar_session, UnitType, Indicator, Exchange, \
    TickerCategory, Sector, Scale, Currency, Ticker


def load_sharadar_refdata():
    init_quandl()
    session = create_sharadar_session()
    load_indicators(session)
    load_tickers(session)
    session.commit()


def load_indicators(session: Session):
    df = quandl.get_table("SHARADAR/INDICATORS", paginate=True)
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


# noinspection DuplicatedCode
def load_tickers(session: Session):
    df = quandl.get_table("SHARADAR/TICKERS", paginate=True)

    for index, row in df.iterrows():
        table_name = row['table']
        perma_ticker_id = row['permaticker']
        ticker = row['ticker']
        if ticker == 'N/A':
            ticker = None
        name = row['name']
        exchange = Exchange.get_or_create(session, row['exchange'])
        is_delisted = yes_no_to_bool(row['isdelisted'])
        category = TickerCategory.get_or_create(session, row['category'])

        cusips = row['cusips']

        sic_sector_code = row['siccode']
        sic_sector = row['sicsector']
        sic_industry = row['sicindustry']
        sic_sector_map = Sector.get_or_create(session, sector_code_type_code='SIC', sector_code=sic_sector_code,
                                              sector=sic_sector, industry=sic_industry)

        fama_sector = row['famasector']
        fama_industry = row['famaindustry']
        fama_sector_map = Sector.get_or_create(session, sector_code_type_code='FAMA', sector_code=None,
                                               sector=fama_sector, industry=fama_industry)

        sector = row['sector']
        industry = row['industry']
        sharadar_sector_map = Sector.get_or_create(session, sector_code_type_code='Sharadar', sector_code=None,
                                                   sector=sector, industry=industry)

        market_cap_scale_code = row['scalemarketcap']
        market_cap_scale = Scale.get_or_create(session, market_cap_scale_code)

        revenue_scale_code = row['scalerevenue']
        revenue_scale = Scale.get_or_create(session, revenue_scale_code)

        related_tickers = row['relatedtickers']

        currency_code = row['currency']
        currency = Currency.get_or_create(session, currency_code)

        location = row['location']
        last_updated = clean_date(row['lastupdated'])
        first_added = clean_date(row['firstadded'])
        first_price_date = clean_date(row['firstpricedate'])
        last_price_date = clean_date(row['lastpricedate'])
        first_quarter = clean_date(row['firstquarter'])
        last_quarter = clean_date(row['lastquarter'])
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

            session.add(ticker_entity)
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
            pass

    session.commit()


def yes_no_to_bool(yes_no: str) -> bool:
    return yes_no == 'Y'


def clean_date(date_value):
    if pd.isnull(date_value):
        return None
    else:
        return date_value


if __name__ == '__main__':
    fire.Fire(load_sharadar_refdata)
