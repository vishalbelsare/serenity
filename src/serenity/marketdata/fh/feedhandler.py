import asyncio
from decimal import Decimal

import capnp
import cryptofeed
import logging

from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum, auto
from pathlib import Path
from typing import List

# noinspection PyProtectedMember
import zmq
from cryptofeed.callback import TradeCallback, BookUpdateCallback
from cryptofeed.defines import TRADES, BOOK_DELTA, L2_BOOK, BID, ASK

# noinspection PyProtectedMember
from prometheus_client import start_http_server, Counter
from tau.core import Signal, MutableSignal, NetworkScheduler, Event, Network, RealtimeNetworkScheduler
from tau.event import Do
from tau.signal import Function

from serenity.app.base import Application
from serenity.app.daemon import ZeroMQPublisher
from serenity.db.api import TypeCodeCache, InstrumentCache, connect_serenity_db
from serenity.marketdata.api import MarketdataService, OrderBook, OrderBookSnapshot
from serenity.marketdata.fh.txlog import TransactionLog
from serenity.model.exchange import ExchangeInstrument
from serenity.trading.api import Side

common_capnp_path = Path(__file__).parent / '../common.capnp'
feedhandler_capnp_path = Path(__file__).parent / 'feedhandler.capnp'

capnp.remove_import_hook()
common_capnp = capnp.load(str(common_capnp_path))
feedhandler_capnp = capnp.load(str(feedhandler_capnp_path))


class FeedHandlerState(Enum):
    """
    Supported lifecycle states for a FeedHandler. FeedHandlers always start in INITIALIZING state.
    """

    INITIALIZING = auto()
    STARTING = auto()
    LIVE = auto()
    STOPPED = auto()


class Feed:
    """
    A marketdata feed with ability to subscribe to trades and quotes
    """

    def __init__(self, instrument: ExchangeInstrument, trades: Signal, order_book_events: Signal, order_books: Signal):
        self.instrument = instrument
        self.trades = trades
        self.order_book_events = order_book_events
        self.order_books = order_books

    def get_instrument(self) -> ExchangeInstrument:
        """
        Gets the trading instrument for which we are feeding data.
        """
        return self.instrument

    def get_trades(self) -> Signal:
        """
        Gets all trade prints for this instrument on the connected exchange.
        """
        return self.trades

    def get_order_book_events(self) -> Signal:
        """
        Gets a stream of OrderBookEvents. Note at this time there is no automatic snapshotting so you are
        likely to get only OrderBookUpdate from here.
        """
        return self.order_book_events

    def get_order_books(self) -> Signal:
        """
        Gets a stream of fully-built L2 OrderBook objects.
        """
        return self.order_books


class FeedHandler(ABC):
    """
    A connector for exchange marketdata.
    """

    @staticmethod
    def get_uri_scheme() -> str:
        """
        Gets the short string name like 'phemex' or 'kraken' for this feedhandler.
        """
        pass

    @abstractmethod
    def get_instance_id(self) -> str:
        """
        Gets the specific instance connected to, e.g. 'prod' or 'test'
        """
        pass

    @abstractmethod
    def get_instruments(self) -> List[ExchangeInstrument]:
        """
        Gets the instruments supported by this feedhandler.
        """
        pass

    @abstractmethod
    def get_state(self) -> Signal:
        """
        Gets a stream of FeedHandlerState enums that updates as the FeedHandler transitions between states.
        """
        pass

    @abstractmethod
    def get_feed(self, uri: str) -> Feed:
        """
        Acquires a feed for the given URI of the form scheme:instance:instrument_id, e.g.
        phemex:prod:BTCUSD or coinbase:test:BTC-USD. Raises an exception if the scheme:instance
        portion does not match this FeedHandler.
        """
        pass

    @abstractmethod
    async def start(self):
        """
        Starts the subscription to the exchange
        """
        pass


class FeedHandlerDaemon(ZeroMQPublisher, ABC):
    """
    Base class for all new-style feedhandlers that run as daemons only
    and distribute data via ZeroMQ sockets.
    """
    def __init__(self, config_path: str):
        super().__init__(config_path)
        self.trade_counter = Counter('trade_counter',
                                     'Number of trade prints received by feedhandler')
        self.book_update_counter = Counter('book_update_counter',
                                           'Number of book updates received by feedhandler')

    @abstractmethod
    def get_feed_code(self):
        pass

    def start_services(self):
        service_id = self._get_fully_qualified_service_id('publisher')
        meta = {'protocol': 'ZMQ', 'feed': self.get_feed_code()}
        self.pub_socket = self._bind_socket(zmq.PUB, 'feedhandler-publisher', service_id, tags=None, meta=meta)

    def _publish_trade_print(self, symbol: str, trade_id: str, timestamp: float, side: str, amount: Decimal,
                             price: Decimal, receipt_timestamp: float):
        msg = feedhandler_capnp.TradeMessage.new_message()
        msg.time = receipt_timestamp
        msg.exchTime = timestamp
        msg.symbol = common_capnp.Symbol.new_message()
        msg.symbol.feedCode = self.get_feed_code()
        msg.symbol.symbolCode = symbol
        msg.tradeId = trade_id
        msg.side = common_capnp.Side.buy if side == 'buy' else common_capnp.Side.sell
        msg.size = float(amount)
        msg.price = float(price)

        asyncio.ensure_future(self._publish_msg('trades', msg.to_bytes_packed()))

        self.trade_counter.inc()

    def _publish_book_delta(self, symbol: str, delta: dict, timestamp: float, receipt_timestamp: float):
        msg = feedhandler_capnp.OrderBookDeltaMessage.new_message()
        msg.time = receipt_timestamp
        msg.exchTime = timestamp
        msg.symbol = common_capnp.Symbol.new_message()
        msg.symbol.feedCode = self.get_feed_code()
        msg.symbol.symbolCode = symbol

        msg.init('bidDeltas', len(delta[BID]))
        FeedHandlerDaemon._to_price_levels(delta[BID], msg.bidDeltas)

        msg.init('askDeltas', len(delta[ASK]))
        FeedHandlerDaemon._to_price_levels(delta[ASK], msg.askDeltas)

        asyncio.ensure_future(self._publish_msg('book_deltas', msg.to_bytes_packed()))

        self.book_update_counter.inc()

    @staticmethod
    def _to_price_levels(level_tuples: list, price_levels: list):
        i = 0
        for level_tuple in level_tuples:
            price_levels[i].px = float(level_tuple[0])
            price_levels[i].qty = float(level_tuple[1])
            i = i + 1


class CryptofeedFeedHandler(FeedHandlerDaemon, ABC):
    """
    Base class for all feedhandlers that use cryptofeed to subscribe to market data.
    """
    def __init__(self, config_path: str):
        super().__init__(config_path)
        self.cryptofeed = cryptofeed.FeedHandler()

    def start_services(self):
        super().start_services()
        self._subscribe(self.cryptofeed)
        self.cryptofeed.run(start_loop=False)

    @abstractmethod
    def _create_feed(self, subscription: dict, callbacks: dict):
        pass

    @abstractmethod
    def _load_symbols(self):
        pass

    def _subscribe(self, f: cryptofeed.FeedHandler):
        symbols = self._load_symbols()
        self.logger.info(f'preparing to subscribe to {len(symbols)} symbols:')
        for symbol in symbols:
            self.logger.info(f'\t{symbol}')

        subscription = {TRADES: symbols, L2_BOOK: symbols}
        callbacks = {
            TRADES: TradeCallback(self._on_trade, include_order_type=False),
            BOOK_DELTA: BookUpdateCallback(self._on_book_delta)
        }
        f.add_feed(self._create_feed(subscription=subscription, callbacks=callbacks))

    # noinspection PyUnusedLocal
    async def _on_trade(self, feed: str, symbol: str, order_id, timestamp: float, side: str, amount: Decimal,
                        price: Decimal, receipt_timestamp: float, order_type: str = None):
        self._publish_trade_print(symbol, order_id, timestamp, side, amount, price, receipt_timestamp)

    # noinspection PyUnusedLocal
    async def _on_book_delta(self, feed: str, symbol: str, delta: dict, timestamp: float, receipt_timestamp: float):
        self._publish_book_delta(symbol, delta, timestamp, receipt_timestamp)


class WebsocketFeedHandler(FeedHandler, ABC):
    """
    Base class for feedhandlers which implement raw websocket-based
    connections.

    This base class is only used for feedhandlers that are not
    currently supported by the cryptofeed package, e.g. Phemex.
    This code is currently being refactored and the "legacy"
    feedhandlers do not yet have the same functionality as the
    newer cryptofeed-based feedhandlers.
    """
    logger = logging.getLogger(__name__)

    def __init__(self, scheduler: NetworkScheduler, instrument_cache: InstrumentCache, instance_id: str):
        self.scheduler = scheduler
        self.instrument_cache = instrument_cache
        self.type_code_cache = instrument_cache.get_type_code_cache()
        self.instance_id = instance_id

        self.instruments = []
        self.known_instrument_ids = {}
        self.price_scaling = {}
        self._load_instruments()

        self.state = MutableSignal(FeedHandlerState.INITIALIZING)
        self.scheduler.get_network().attach(self.state)

    def get_instance_id(self) -> str:
        return self.instance_id

    def get_instruments(self) -> List[ExchangeInstrument]:
        return self.instruments

    def get_state(self) -> Signal:
        return self.state

    def get_feed(self, uri: str) -> Feed:
        (scheme, instance_id, instrument_id) = uri.split(':')
        if scheme != self.get_uri_scheme():
            raise ValueError(f'Unsupported URI scheme: {scheme}')
        if instance_id != self.get_instance_id():
            raise ValueError(f'Unsupported instance ID: {instance_id}')
        if instrument_id not in self.known_instrument_ids:
            raise ValueError(f'Unknown exchange Instrument: {instrument_id}')
        return self._create_feed(self.known_instrument_ids[instrument_id])

    async def start(self):
        self.scheduler.schedule_update(self.state, FeedHandlerState.STARTING)
        await self._subscribe_trades_and_quotes()

    @abstractmethod
    def _create_feed(self, instrument: ExchangeInstrument):
        pass

    @abstractmethod
    def _load_instruments(self):
        pass

    @abstractmethod
    async def _subscribe_trades_and_quotes(self):
        pass


class FeedHandlerRegistry:
    """
    A central registry of all known FeedHandlers.
    """

    logger = logging.getLogger(__name__)

    def __init__(self):
        self.fh_registry = {}
        self.feeds = {}

    def get_feedhandlers(self) -> List[FeedHandler]:
        return list(self.fh_registry.values())

    def get_feed(self, uri: str) -> Feed:
        """
        Acquires a Feed for a FeedHandler based on a URI of the form scheme:instrument:instrument_id,
        e.g. phemex:prod:BTCUSD or coinbase:test:BTC-USD. Raises an exception if there is no
        registered handler for the given URI.
        """
        if uri in self.feeds:
            return self.feeds[uri]
        else:
            (scheme, instance, instrument_id) = uri.split(':')
            fh = self.fh_registry[f'{scheme}:{instance}']
            if not fh:
                raise ValueError(f'Unknown FeedHandler URI: {uri}')

            feed = fh.get_feed(uri)
            self.feeds[uri] = feed
            return feed

    def register(self, feedhandler: FeedHandler):
        """
        Registers a FeedHandler so its feeds can be acquired centrally with get_feed().
        """
        fh_key = FeedHandlerRegistry._get_fh_key(feedhandler)
        self.fh_registry[fh_key] = feedhandler
        self.logger.info(f'registered FeedHandler: {fh_key}')

    @staticmethod
    def _get_fh_key(feedhandler: FeedHandler) -> str:
        return f'{feedhandler.get_uri_scheme()}:{feedhandler.get_instance_id()}'


class OrderBookBuilder(Function):
    def __init__(self, network: Network, events: Signal):
        super().__init__(network, events)
        self.events = events
        self.order_book = OrderBook([], [])

    def _call(self):
        if self.events.is_valid():
            next_event = self.events.get_value()
            if isinstance(next_event, OrderBookSnapshot):
                self.order_book = OrderBook(next_event.get_bids(), next_event.get_asks())
            else:
                self.order_book.apply_order_book_update(next_event)

            self._update(self.order_book)


class FeedHandlerMarketdataService(MarketdataService):
    """
    MarketdataService implementation that uses embedded FeedHandler instances
    via the FeedHandlerRegistry to subscribe to marketdata streams.
    """

    def __init__(self, scheduler: NetworkScheduler, registry: FeedHandlerRegistry, instance_id: str = 'prod'):
        self.scheduler = scheduler
        self.registry = registry
        self.instance_id = instance_id
        self.subscribed_instruments = MutableSignal()
        self.notified_instruments = set()
        scheduler.get_network().attach(self.subscribed_instruments)

    def get_subscribed_instruments(self) -> Signal:
        return self.subscribed_instruments

    def get_order_book_events(self, instrument: ExchangeInstrument) -> Signal:
        order_book_events = self.registry.get_feed(self.__get_feed_uri(instrument)).get_order_book_events()
        return order_book_events

    def get_order_books(self, instrument: ExchangeInstrument) -> Signal:
        order_books = self.registry.get_feed(self.__get_feed_uri(instrument)).get_order_books()
        return order_books

    def get_trades(self, instrument: ExchangeInstrument) -> Signal:
        trades = self.registry.get_feed(self.__get_feed_uri(instrument)).get_trades()
        return trades

    def __get_feed_uri(self, instrument: ExchangeInstrument) -> str:
        if instrument not in self.notified_instruments:
            self.notified_instruments.add(instrument)
            self.scheduler.schedule_update(self.subscribed_instruments, instrument)
        symbol = instrument.get_exchange_instrument_code()
        return f'{instrument.get_exchange().get_exchange_code().lower()}:{self.instance_id}:{symbol}'


def ws_fh_main(create_fh, uri_scheme: str, instance_id: str, journal_path: str, db: str, journal_books: bool = True,
               include_symbol: str = '*'):
    Application.init_logging()
    logger = logging.getLogger(__name__)

    conn = connect_serenity_db()
    conn.autocommit = True
    cur = conn.cursor()

    instr_cache = InstrumentCache(cur, TypeCodeCache(cur))

    scheduler = RealtimeNetworkScheduler()
    registry = FeedHandlerRegistry()
    fh = create_fh(scheduler, instr_cache, include_symbol, instance_id)
    registry.register(fh)

    # register Prometheus metrics
    trade_counter = Counter('serenity_trade_counter', 'Number of trade prints received by feedhandler')
    book_update_counter = Counter('serenity_book_update_counter', 'Number of book updates received by feedhandler')

    for instrument in fh.get_instruments():
        symbol = instrument.get_exchange_instrument_code()
        if not (symbol == include_symbol or include_symbol == '*'):
            continue

        # subscribe to FeedState in advance so we know when the Feed is ready to subscribe trades
        class SubscribeTrades(Event):
            def __init__(self, trade_symbol):
                self.trade_symbol = trade_symbol
                self.tx_writer = None

            def on_activate(self) -> bool:
                if fh.get_state().get_value() == FeedHandlerState.LIVE:
                    feed = registry.get_feed(f'{uri_scheme}:{instance_id}:{self.trade_symbol}')
                    instrument_code = feed.get_instrument().get_exchange_instrument_code()
                    txlog = TransactionLog(Path(f'{journal_path}/{db}_TRADES/{instrument_code}'))
                    self.tx_writer = txlog.create_writer()

                    trades = feed.get_trades()
                    Do(scheduler.get_network(), trades, lambda: self.on_trade_print(trades.get_value()))
                return False

            def on_trade_print(self, trade):
                trade_counter.inc()
                logger.info(trade)

                trade_msg = feedhandler_capnp.TradeMessage.new_message()
                trade_msg.time = datetime.utcnow().timestamp()
                trade_msg.tradeId = trade.get_trade_id()
                trade_msg.side = feedhandler_capnp.Side.buy if trade.get_side() == Side.BUY \
                    else feedhandler_capnp.Side.sell
                trade_msg.size = trade.get_qty()
                trade_msg.price = trade.get_price()

                self.tx_writer.append_msg(trade_msg)

        if journal_books:
            class SubscribeOrderBook(Event):
                def __init__(self, trade_symbol):
                    self.trade_symbol = trade_symbol
                    self.tx_writer = None

                def on_activate(self) -> bool:
                    if fh.get_state().get_value() == FeedHandlerState.LIVE:
                        feed = registry.get_feed(f'{uri_scheme}:{instance_id}:{self.trade_symbol}')
                        instrument_code = feed.get_instrument().get_exchange_instrument_code()
                        txlog = TransactionLog(Path(f'{journal_path}/{db}_BOOKS/{instrument_code}'))
                        self.tx_writer = txlog.create_writer()

                        books = feed.get_order_books()
                        Do(scheduler.get_network(), books, lambda: self.on_book_update(books.get_value()))
                    return False

                def on_book_update(self, book: OrderBook):
                    book_update_counter.inc()

                    book_msg = feedhandler_capnp.Level1BookUpdateMessage.new_message()
                    book_msg.time = datetime.utcnow().timestamp()
                    if len(book.get_bids()) > 0:
                        book_msg.bestBidQty = book.get_best_bid().get_qty()
                        book_msg.bestBidPx = book.get_best_bid().get_px()
                    else:
                        book_msg.bestBidQty = 0
                        book_msg.bestBidPx = 0

                    if len(book.get_asks()) > 0:
                        book_msg.bestAskQty = book.get_best_ask().get_qty()
                        book_msg.bestAskPx = book.get_best_ask().get_px()
                    else:
                        book_msg.bestAskQty = 0
                        book_msg.bestAskPx = 0

                    self.tx_writer.append_msg(book_msg)

            scheduler.get_network().connect(fh.get_state(), SubscribeOrderBook(symbol))

        scheduler.get_network().connect(fh.get_state(), SubscribeTrades(symbol))

    # launch the monitoring endpoint
    start_http_server(8000)

    # async start the feedhandler
    asyncio.ensure_future(fh.start())

    # go!
    asyncio.get_event_loop().run_forever()
