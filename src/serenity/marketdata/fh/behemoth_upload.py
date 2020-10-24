import datetime
import logging
import os
from pathlib import Path

import fire
import pandas as pd

from serenity.db.api import connect_serenity_db, InstrumentCache, TypeCodeCache
from serenity.tickstore.journal import Journal, NoSuchJournalException
from serenity.tickstore.tickstore import LocalTickstore, BiTimestamp, AzureBlobTickstore
from serenity.utils import init_logging


# noinspection DuplicatedCode
def upload_main(behemoth_path: str = '/behemoth', days_back: int = 1):
    logger = logging.getLogger(__name__)
    upload_date = datetime.datetime.utcnow().date() - datetime.timedelta(days_back)

    conn = connect_serenity_db()
    conn.autocommit = True
    cur = conn.cursor()
    instr_cache = InstrumentCache(cur, TypeCodeCache(cur))

    exchanges = {
        'PHEMEX': 'PHEMEX',
        'COINBASE_PRO': 'COINBASE'
    }
    for exchange, db_prefix in exchanges.items():
        for instrument in instr_cache.get_all_exchange_instruments(exchange):
            symbol = instrument.get_exchange_instrument_code()

            upload_trades(behemoth_path, db_prefix, exchange, logger, symbol, upload_date)
            upload_order_books(behemoth_path, db_prefix, exchange, logger, symbol, upload_date)


# noinspection DuplicatedCode
def upload_order_books(behemoth_path, db_prefix, exchange, logger, symbol, upload_date):
    books_db = f'{db_prefix}_BOOKS'
    books_path = Path(f'{behemoth_path}/journals/{books_db}/{symbol}')
    books_journal = Journal(books_path)
    try:
        reader = books_journal.create_reader(upload_date)

        length = reader.get_length()
        records = []
        while reader.get_pos() < length:
            time = reader.read_double()

            best_bid_qty = reader.read_long()
            best_bid_px = reader.read_double()
            best_ask_qty = reader.read_long()
            best_ask_px = reader.read_double()

            record = {
                'time': datetime.datetime.fromtimestamp(time),
                'best_bid_qty': best_bid_qty,
                'best_bid_px': best_bid_px,
                'best_ask_qty': best_ask_qty,
                'best_ask_px': best_ask_px
            }
            records.append(record)

        if len(records) > 0:
            logger.info(
                f'uploading journaled {exchange}/{symbol} books to Behemoth for UTC date {str(upload_date)}')
            df = pd.DataFrame(records)
            df.set_index('time', inplace=True)
            logger.info(f'extracted {len(df)} {symbol} order books')
            tickstore = LocalTickstore(Path(Path(f'{behemoth_path}/db/{books_db}')), 'time')
            tickstore.insert(symbol, BiTimestamp(upload_date), df)
            tickstore.close()
            logger.info(f'inserted {len(df)} {symbol} order book records on local disk')

            cloud_tickstore = connect_azure_blob_tickstore(books_db)
            cloud_tickstore.insert(symbol, BiTimestamp(upload_date), df)
            cloud_tickstore.close()
            logger.info(f'inserted {len(df)} {symbol} order book records in cloud storage')
        else:
            logger.info(f'zero {exchange}/{symbol} books for UTC date {str(upload_date)}')
            tickstore = LocalTickstore(Path(Path(f'{behemoth_path}/db/{books_db}')), 'time')
            tickstore.close()
    except NoSuchJournalException:
        logger.error(f'missing journal file: {books_path}')


# noinspection DuplicatedCode
def upload_trades(behemoth_path, db_prefix, exchange, logger, symbol, upload_date):
    trades_db = f'{db_prefix}_TRADES'
    trades_path = Path(f'{behemoth_path}/journals/{trades_db}/{symbol}')
    trades_journal = Journal(trades_path)
    try:
        reader = trades_journal.create_reader(upload_date)

        length = reader.get_length()
        records = []
        while reader.get_pos() < length:
            time = reader.read_double()
            sequence = reader.read_long()
            trade_id = reader.read_long()
            product_id = reader.read_string()
            side = 'buy' if reader.read_short() == 0 else 'sell'
            size = reader.read_double()
            price = reader.read_double()

            record = {
                'time': datetime.datetime.fromtimestamp(time),
                'sequence': sequence,
                'trade_id': trade_id,
                'product_id': product_id,
                'side': side,
                'size': size,
                'price': price
            }
            records.append(record)

        if len(records) > 0:
            logger.info(
                f'uploading journaled {exchange}/{symbol} ticks to Behemoth for UTC date {str(upload_date)}')
            df = pd.DataFrame(records)
            df.set_index('time', inplace=True)
            logger.info(f'extracted {len(df)} {symbol} trade records')
            tickstore = LocalTickstore(Path(Path(f'{behemoth_path}/db/{trades_db}')), 'time')
            tickstore.insert(symbol, BiTimestamp(upload_date), df)
            tickstore.close()
            logger.info(f'inserted {len(df)} {symbol} trade records on local disk')

            cloud_tickstore = connect_azure_blob_tickstore(trades_db)
            cloud_tickstore.insert(symbol, BiTimestamp(upload_date), df)
            cloud_tickstore.close()
            logger.info(f'inserted {len(df)} {symbol} trade records in cloud storage')
        else:
            logger.info(f'zero {exchange}/{symbol} ticks for UTC date {str(upload_date)}')
            tickstore = LocalTickstore(Path(Path(f'{behemoth_path}/db/{trades_db}')), 'time')
            tickstore.close()

            cloud_tickstore = connect_azure_blob_tickstore(trades_db)
            cloud_tickstore.close()

    except NoSuchJournalException:
        logger.error(f'missing journal file: {trades_path}')


def connect_azure_blob_tickstore(db: str):
    connect_str = os.getenv('AZURE_CONNECT_STR', None)
    return AzureBlobTickstore(connect_str, db)


if __name__ == '__main__':
    fire.Fire(upload_main)
