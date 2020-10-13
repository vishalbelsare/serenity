import logging

import pandas_market_calendars
import polygon

from datetime import datetime

from tau.core import Signal, HistoricNetworkScheduler, MutableSignal, NetworkScheduler, SignalGenerator

from serenity.marketdata.api import MarketdataService, Trade, OrderBook, BookLevel
from serenity.model.exchange import ExchangeInstrument
from serenity.tickstore.tickstore import AzureBlobTickstore
from serenity.trading.api import Side


class AzureHistoricMarketdataService(MarketdataService):
    """
    A historical replay service based off of Serenity's native AzureBlobTickstore.
    """

    def __init__(self, scheduler: HistoricNetworkScheduler, azure_connect_str: str):
        self.scheduler = scheduler
        self.azure_connect_str = azure_connect_str
        self.subscribed_instruments = MutableSignal()
        self.scheduler.get_network().attach(self.subscribed_instruments)

        self.start_time = datetime.fromtimestamp(scheduler.get_start_time() / 1000.0)
        self.end_time = datetime.fromtimestamp(scheduler.get_end_time() / 1000.0)

        self.all_subscribed = set()
        self.book_signal_by_symbol = {}
        self.trade_signal_by_symbol = {}

    def get_subscribed_instruments(self) -> Signal:
        return self.subscribed_instruments

    def get_order_book_events(self, instrument: ExchangeInstrument) -> Signal:
        raise NotImplementedError()

    def get_order_books(self, instrument: ExchangeInstrument) -> Signal:
        symbol = instrument.get_exchange_instrument_code()
        exchange_code_upper = instrument.get_exchange().get_exchange_code().upper()

        order_books = self.book_signal_by_symbol.get(symbol, None)
        if order_books is None:
            self._maybe_notify_subscription(instrument)

            order_books = MutableSignal()
            self.scheduler.network.attach(order_books)
            self.book_signal_by_symbol[symbol] = order_books

            class OrderBookGenerator(SignalGenerator):
                def __init__(self, mds):
                    self.mds = mds

                def generate(self, scheduler: NetworkScheduler):
                    tickstore = AzureBlobTickstore(self.mds.azure_connect_str, f'{exchange_code_upper}_BOOKS',
                                                   timestamp_column='time')
                    books_df = tickstore.select(symbol, self.mds.start_time, self.mds.end_time)
                    for row in books_df.itertuples():
                        at_time = row[0].asm8.astype('int64') / 10**6
                        best_bid = BookLevel(row[2], row[1])
                        best_ask = BookLevel(row[4], row[3])
                        book = OrderBook(bids=[best_bid], asks=[best_ask])
                        scheduler.schedule_update_at(order_books, book, at_time)
                    tickstore.close()

            self.scheduler.add_generator(OrderBookGenerator(self))

            # workaround: force scheduling of all historic trade events
            self.get_trades(instrument)

        return order_books

    def get_trades(self, instrument: ExchangeInstrument) -> Signal:
        trade_symbol = instrument.get_exchange_instrument_code()
        exchange_code_upper = instrument.get_exchange().get_exchange_code().upper()

        self._maybe_notify_subscription(instrument)

        trades = self.trade_signal_by_symbol.get(trade_symbol, None)
        if trades is None:
            trades = MutableSignal()
            self.scheduler.network.attach(trades)
            self.trade_signal_by_symbol[trade_symbol] = trades

            class TradeGenerator(SignalGenerator):
                def __init__(self, mds):
                    self.mds = mds

                def generate(self, scheduler: NetworkScheduler):
                    tickstore = AzureBlobTickstore(self.mds.azure_connect_str, f'{exchange_code_upper}_TRADES',
                                                   timestamp_column='time')
                    trades_df = tickstore.select(trade_symbol, self.mds.start_time, self.mds.end_time)
                    for row in trades_df.itertuples():
                        at_time = row[0].asm8.astype('int64') / 10 ** 6
                        sequence = row[1]
                        trade_id = row[2]
                        side = Side.SELL if row[4] == 'sell' else Side.BUY
                        qty = row[5]
                        price = row[6]
                        trade = Trade(instrument, sequence, trade_id, side, qty, price)
                        scheduler.schedule_update_at(trades, trade, at_time)

                    tickstore.close()

            self.scheduler.add_generator(TradeGenerator(self))

            # workaround: force scheduling of all historic order book events
            self.get_order_books(instrument)

        return trades

    def _maybe_notify_subscription(self, instrument: ExchangeInstrument):
        if instrument not in self.all_subscribed:
            self.scheduler.schedule_update(self.subscribed_instruments, instrument)
            self.all_subscribed.add(instrument)


class PolygonHistoricMarketdataService(MarketdataService):
    """
    A historical replay service based off of Polygon.io's REST API.
    """

    logger = logging.getLogger(__name__)

    def __init__(self, scheduler: HistoricNetworkScheduler, api_key: str):
        self.scheduler = scheduler
        self.subscribed_instruments = MutableSignal()
        self.scheduler.get_network().attach(self.subscribed_instruments)

        self.all_subscribed = set()
        self.trade_signal_by_symbol = {}

        self.client = polygon.RESTClient(api_key)
        self.start_date = datetime.fromtimestamp(scheduler.get_start_time() / 1000.0).date()
        self.end_date = datetime.fromtimestamp(scheduler.get_end_time() / 1000.0).date()

    def get_subscribed_instruments(self) -> Signal:
        return self.subscribed_instruments

    def get_order_book_events(self, instrument: ExchangeInstrument) -> Signal:
        raise NotImplementedError()

    def get_order_books(self, instrument: ExchangeInstrument) -> Signal:
        raise NotImplementedError()

    # noinspection PyUnresolvedReferences
    def get_trades(self, instrument: ExchangeInstrument) -> Signal:
        trade_symbol = instrument.get_exchange_instrument_code()
        trades = self.trade_signal_by_symbol.get(trade_symbol, None)
        if trades is None:
            trades = MutableSignal()
            self.scheduler.network.attach(trades)
            self.trade_signal_by_symbol[trade_symbol] = trades

            calendar = pandas_market_calendars.get_calendar(instrument.get_exchange().get_exchange_calendar())
            tz = instrument.get_exchange().get_exchange_tz()
            exch_schedule_df = calendar.schedule(self.start_date, self.end_date, tz)
            for row in exch_schedule_df.itertuples():
                load_date = row[0].to_pydatetime().date()
                market_open = int(row[1].to_pydatetime().timestamp() * 1000 * 1000 * 1000)
                market_close = int(row[2].to_pydatetime().timestamp() * 1000 * 1000 * 1000)

                timestamp = market_open
                while timestamp < market_close:
                    self.logger.info(f'Starting historic trade download from timestamp={timestamp}')
                    symbol = instrument.get_exchange_instrument_code()
                    if instrument.get_instrument().get_instrument_type() == 'CryptoCurrencyPair':
                        ccy_pair = instrument.get_instrument().get_economics()
                        base_ccy = ccy_pair.get_base_ccy().get_currency_code()
                        quote_ccy = ccy_pair.get_quote_ccy().get_currency_code()
                        resp = self.client.crypto_historic_crypto_trades(base_ccy, quote_ccy, load_date,
                                                                         timestamp=timestamp)
                    elif instrument.get_instrument().get_instrument_type() == 'FX':
                        ccy_pair = instrument.get_instrument().get_economics()
                        base_ccy = ccy_pair.get_base_ccy().get_currency_code()
                        quote_ccy = ccy_pair.get_quote_ccy().get_currency_code()
                        resp = self.client.forex_currencies_historic_forex_ticks(base_ccy, quote_ccy, load_date,
                                                                                 timestamp=timestamp)
                    else:
                        resp = self.client.historic_trades_v2(symbol, load_date, timestamp=timestamp)

                        for v2_trade in resp.results:
                            at_time = int(v2_trade['t'] / 1000 / 1000)
                            sequence = v2_trade['q']
                            trade_id = v2_trade['i']
                            qty = v2_trade['s']
                            price = v2_trade['p']

                            trade = Trade(instrument, sequence, trade_id, Side.UNKNOWN, qty, price)
                            self.scheduler.schedule_update_at(trades, trade, at_time)

                        last_trade = resp.results[-1]
                        timestamp = last_trade['t']
        return trades

    def _maybe_notify_subscription(self, instrument: ExchangeInstrument):
        if instrument not in self.all_subscribed:
            self.scheduler.schedule_update(self.subscribed_instruments, instrument)
            self.all_subscribed.add(instrument)
