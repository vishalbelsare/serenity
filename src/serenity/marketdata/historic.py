import pandas as pd

from datetime import datetime
from typing import List

from tau.core import Signal, HistoricNetworkScheduler, MutableSignal

from serenity.marketdata.api import MarketdataService, Trade, OrderBook, BookLevel
from serenity.model.exchange import ExchangeInstrument
from serenity.tickstore.tickstore import AzureBlobTickstore
from serenity.trading.api import Side


class HistoricMarketdataService(MarketdataService):
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
