import logging

import fire
import pandas as pd
import quandl

from serenity.equity.sharadar_api import init_quandl, create_sharadar_session, clean_nulls, yes_no_to_bool
from serenity.equity.sharadar_holdings import FormType, SecurityAdType, TransactionType, SecurityTitleType, \
    InsiderHoldings
from serenity.equity.sharadar_refdata import Ticker
from serenity.utils import init_logging

logger = logging.getLogger(__name__)


# noinspection DuplicatedCode
def load_sharadar_insider_holdings():
    session = create_sharadar_session()

    load_path = 'sharadar_insider_holdings.zip'
    logger.info(f'downloading insider holdings data to {load_path}')
    quandl.export_table('SHARADAR/SF2', filename=load_path)
    df = pd.read_csv(load_path)
    logger.info(f'loaded {len(df)} rows of insider holdings CSV data from {load_path}')

    row_count = 0
    for index, row in df.iterrows():
        ticker_code = row['ticker']
        ticker = Ticker.find_by_ticker(session, ticker_code)
        if ticker is None:
            logger.warning(f'unknown ticker referenced; skipping: {ticker_code}')
            continue

        filing_date = row['filingdate']

        form_type_code = row['formtype']
        form_type = FormType.get_or_create(session, form_type_code)

        issuer_name = row['issuername']
        owner_name = row['ownername']
        officer_title = row['officertitle']
        is_director = yes_no_to_bool(row['isdirector'])
        is_officer = yes_no_to_bool(row['isofficer'])
        is_ten_percent_owner = yes_no_to_bool(row['istenpercentowner'])
        transaction_date = clean_nulls(row['transactiondate'])

        security_ad_type_code = clean_nulls(row['securityadcode'])
        security_ad_type = SecurityAdType.get_or_create(session, security_ad_type_code)

        transaction_type_code = clean_nulls(row['transactioncode'])
        transaction_type = TransactionType.get_or_create(session, transaction_type_code)

        shares_owned_before_transaction = clean_nulls(row['sharesownedbeforetransaction'])
        transaction_shares = clean_nulls(row['transactionshares'])
        shares_owned_following_transaction = clean_nulls(row['sharesownedfollowingtransaction'])
        transaction_price_per_share = clean_nulls(row['transactionpricepershare'])
        transaction_value = clean_nulls(row['transactionvalue'])

        security_title_type_code = row['securitytitle']
        security_title_type = SecurityTitleType.get_or_create(session, security_title_type_code)

        direct_or_indirect = row['directorindirect']
        nature_of_ownership = clean_nulls(row['natureofownership'])
        date_exercisable = clean_nulls(row['dateexercisable'])
        price_exercisable = clean_nulls(row['priceexercisable'])
        expiration_date = clean_nulls(row['expirationdate'])
        row_num = row['rownum']

        holdings = InsiderHoldings.find_holdings(session, ticker, filing_date, owner_name, form_type, row_num)
        if holdings is None:
            holdings = InsiderHoldings(ticker=ticker, filing_date=filing_date, form_type=form_type,
                                       issuer_name=issuer_name, owner_name=owner_name, officer_title=officer_title,
                                       is_director=is_director, is_officer=is_officer,
                                       is_ten_percent_owner=is_ten_percent_owner, transaction_date=transaction_date,
                                       security_ad_type=security_ad_type, transaction_type=transaction_type,
                                       shares_owned_before_transaction=shares_owned_before_transaction,
                                       transaction_shares=transaction_shares,
                                       shares_owned_following_transaction=shares_owned_following_transaction,
                                       transaction_price_per_share=transaction_price_per_share,
                                       transaction_value=transaction_value, security_title_type=security_title_type,
                                       direct_or_indirect=direct_or_indirect, nature_of_ownership=nature_of_ownership,
                                       date_exercisable=date_exercisable, price_exercisable=price_exercisable,
                                       expiration_date=expiration_date, row_num=row_num)
        else:
            holdings.issuer_name = issuer_name
            holdings.officer_title = officer_title
            holdings.is_director = is_director
            holdings.is_officer = is_officer
            holdings.is_ten_percent_owner = is_ten_percent_owner
            holdings.transaction_date = transaction_date
            holdings.shares_owned_before_transaction = shares_owned_before_transaction
            holdings.transaction_shares = transaction_shares
            holdings.shares_owned_following_transaction = shares_owned_following_transaction
            holdings.transaction_price_per_share = transaction_price_per_share
            holdings.transaction_value = transaction_value
            holdings.security_title_type = security_title_type
            holdings.direct_or_indirect = direct_or_indirect
            holdings.nature_of_ownership = nature_of_ownership
            holdings.date_exercisable = date_exercisable
            holdings.price_exercisable = price_exercisable
            holdings.expiration_date = expiration_date

        session.add(holdings)

        if row_count > 0 and row_count % 1000 == 0:
            logger.info(f'{row_count} rows loaded; flushing next 1000 rows to database')
            session.commit()
        row_count += 1

    session.commit()


if __name__ == '__main__':
    init_logging()
    init_quandl()
    fire.Fire(load_sharadar_insider_holdings)
