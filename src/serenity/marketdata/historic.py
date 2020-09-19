import pandas as pd

from datetime import datetime
from typing import List

from tau.core import Signal, HistoricNetworkScheduler, MutableSignal

from serenity.marketdata import MarketdataService, Trade, OrderBook, BookLevel
from serenity.model.exchange import ExchangeInstrument
from serenity.tickstore.tickstore import AzureBlobTickstore
from serenity.trading import Side


class HistoricMarketdataService(MarketdataService):
    def __init__(self, scheduler: HistoricNetworkScheduler, instruments_to_cache: List, azure_connect_str: str,
                 start_time_millis: int, end_time_millis: int):
        self.scheduler = scheduler

        start_time = datetime.fromtimestamp(start_time_millis / 1000.0)
        end_time = datetime.fromtimestamp(end_time_millis / 1000.0)

        self.book_signal_by_symbol = {}
        self.trade_signal_by_symbol = {}
        for instrument in instruments_to_cache:
            symbol = instrument.get_exchange_instrument_code()
            exchange_code_upper = instrument.get_exchange().get_type_code().upper()

            tickstore = AzureBlobTickstore(azure_connect_str, f'{exchange_code_upper}_TRADES', timestamp_column='time')
            trades_df = tickstore.select(symbol, start_time, end_time)
            trades_signal = MutableSignal()
            scheduler.network.attach(trades_signal)
            self.trade_signal_by_symbol[symbol] = trades_signal
            for index, row in trades_df.iterrows():
                at_time = pd.to_datetime([index]).astype(int) / 10**6
                sequence = row['sequence']
                trade_id = row['trade_id']
                side = Side.SELL if row['side'] == 'sell' else Side.BUY
                qty = row['size']
                price = row['price']
                trade = Trade(instrument, sequence, trade_id, side, qty, price)
                scheduler.schedule_update_at(trades_signal, trade, at_time)

            tickstore.close()

            tickstore = AzureBlobTickstore(azure_connect_str, f'{exchange_code_upper}_BOOKS', timestamp_column='time')
            books_df = tickstore.select(symbol, start_time, end_time)
            books_signal = MutableSignal()
            scheduler.network.attach(books_signal)
            self.book_signal_by_symbol[symbol] = books_signal
            for index, row in books_df.iterrows():
                at_time = pd.to_datetime([index]).astype(int) / 10**6
                best_bid = BookLevel(row['best_bid_px'], row['best_bid_qty'])
                best_ask = BookLevel(row['best_ask_px'], row['best_ask_qty'])
                book = OrderBook(bids=[best_bid], asks=[best_ask])
                scheduler.schedule_update_at(books_signal, book, at_time)
            tickstore.close()

    def get_order_book_events(self, instrument: ExchangeInstrument) -> Signal:
        raise NotImplementedError()

    def get_order_books(self, instrument: ExchangeInstrument) -> Signal:
        instrument.get_exchange().get_type_code()
        trade_symbol = instrument.get_exchange_instrument_code()
        return self.book_signal_by_symbol[trade_symbol]

    def get_trades(self, instrument: ExchangeInstrument) -> Signal:
        trade_symbol = instrument.get_exchange_instrument_code()
        return self.trade_signal_by_symbol[trade_symbol]
