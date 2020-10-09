import logging

import pandas_market_calendars
import polygon

from datetime import datetime
from typing import List

from tau.core import Signal, HistoricNetworkScheduler, MutableSignal

from serenity.marketdata.api import MarketdataService, Trade, OrderBook, BookLevel
from serenity.model.exchange import ExchangeInstrument
from serenity.tickstore.tickstore import AzureBlobTickstore
from serenity.trading.api import Side


class AzureHistoricMarketdataService(MarketdataService):
    """
    A historical replay service based off of Serenity's native AzureBlobTickstore.
    """

    def __init__(self, scheduler: HistoricNetworkScheduler, instruments_to_cache: List, azure_connect_str: str):
        self.scheduler = scheduler
        self.subscribed_instruments = MutableSignal()
        self.scheduler.get_network().attach(self.subscribed_instruments)

        start_time = datetime.fromtimestamp(scheduler.get_start_time() / 1000.0)
        end_time = datetime.fromtimestamp(scheduler.get_end_time() / 1000.0)

        self.book_signal_by_symbol = {}
        self.trade_signal_by_symbol = {}
        for instrument in instruments_to_cache:
            scheduler.schedule_update(self.subscribed_instruments, instrument)
            symbol = instrument.get_exchange_instrument_code()
            exchange_code_upper = instrument.get_exchange().get_type_code().upper()

            tickstore = AzureBlobTickstore(azure_connect_str, f'{exchange_code_upper}_TRADES', timestamp_column='time')
            trades_df = tickstore.select(symbol, start_time, end_time)
            trades_signal = MutableSignal()
            scheduler.network.attach(trades_signal)
            self.trade_signal_by_symbol[symbol] = trades_signal
            for row in trades_df.itertuples():
                at_time = row[0].asm8.astype('int64') / 10**6
                sequence = row[1]
                trade_id = row[2]
                side = Side.SELL if row[4] == 'sell' else Side.BUY
                qty = row[5]
                price = row[6]
                trade = Trade(instrument, sequence, trade_id, side, qty, price)
                scheduler.schedule_update_at(trades_signal, trade, at_time)

            tickstore.close()

            tickstore = AzureBlobTickstore(azure_connect_str, f'{exchange_code_upper}_BOOKS', timestamp_column='time')
            books_df = tickstore.select(symbol, start_time, end_time)
            books_signal = MutableSignal()
            scheduler.network.attach(books_signal)
            self.book_signal_by_symbol[symbol] = books_signal
            for row in books_df.itertuples():
                at_time = row[0].asm8.astype('int64') / 10**6
                best_bid = BookLevel(row[2], row[1])
                best_ask = BookLevel(row[4], row[3])
                book = OrderBook(bids=[best_bid], asks=[best_ask])
                scheduler.schedule_update_at(books_signal, book, at_time)
            tickstore.close()

    def get_subscribed_instruments(self) -> Signal:
        return self.subscribed_instruments

    def get_order_book_events(self, instrument: ExchangeInstrument) -> Signal:
        raise NotImplementedError()

    def get_order_books(self, instrument: ExchangeInstrument) -> Signal:
        instrument.get_exchange().get_type_code()
        trade_symbol = instrument.get_exchange_instrument_code()
        return self.book_signal_by_symbol[trade_symbol]

    def get_trades(self, instrument: ExchangeInstrument) -> Signal:
        trade_symbol = instrument.get_exchange_instrument_code()
        return self.trade_signal_by_symbol[trade_symbol]


class PolygonHistoricMarketdataService(MarketdataService):
    """
    A historical replay service based off of Polygon.io's REST API.
    """

    logger = logging.getLogger(__name__)

    # noinspection PyUnresolvedReferences
    def __init__(self, scheduler: HistoricNetworkScheduler, instruments_to_cache: List, calendar: str, tz: str,
                 api_key: str):
        self.scheduler = scheduler
        self.subscribed_instruments = MutableSignal()
        self.scheduler.get_network().attach(self.subscribed_instruments)

        self.trade_signal_by_symbol = {}

        client = polygon.RESTClient(api_key)
        calendar = pandas_market_calendars.get_calendar(calendar)
        start_date = datetime.fromtimestamp(scheduler.get_start_time() / 1000.0).date()
        end_date = datetime.fromtimestamp(scheduler.get_end_time() / 1000.0).date()

        for instrument in instruments_to_cache:
            scheduler.schedule_update(self.subscribed_instruments, instrument)
            symbol = instrument.get_exchange_instrument_code()

            trades_signal = MutableSignal()
            scheduler.network.attach(trades_signal)
            self.trade_signal_by_symbol[symbol] = trades_signal

            exch_schedule_df = calendar.schedule(start_date, end_date, tz)
            for row in exch_schedule_df.itertuples():
                load_date = row[0].to_pydatetime().date()
                market_open = int(row[1].to_pydatetime().timestamp() * 1000 * 1000 * 1000)
                market_close = int(row[2].to_pydatetime().timestamp() * 1000 * 1000 * 1000)

                timestamp = market_open
                while timestamp < market_close:
                    self.logger.info(f'Starting historic trade download from timestamp={timestamp}')
                    resp = client.historic_trades_v2(instrument.get_exchange_instrument_code(), load_date,
                                                     timestamp=timestamp)

                    for v2_trade in resp.results:
                        at_time = int(v2_trade['t'] / 1000 / 1000)
                        sequence = v2_trade['q']
                        trade_id = v2_trade['i']
                        qty = v2_trade['s']
                        price = v2_trade['p']

                        trade = Trade(instrument, sequence, trade_id, Side.UNKNOWN, qty, price)
                        scheduler.schedule_update_at(trades_signal, trade, at_time)

                    last_trade = resp.results[-1]
                    timestamp = last_trade['t']

            client.close()

    def get_subscribed_instruments(self) -> Signal:
        return self.subscribed_instruments

    def get_order_book_events(self, instrument: ExchangeInstrument) -> Signal:
        raise NotImplementedError()

    def get_order_books(self, instrument: ExchangeInstrument) -> Signal:
        raise NotImplementedError()

    def get_trades(self, instrument: ExchangeInstrument) -> Signal:
        trade_symbol = instrument.get_exchange_instrument_code()
        return self.trade_signal_by_symbol[trade_symbol]
