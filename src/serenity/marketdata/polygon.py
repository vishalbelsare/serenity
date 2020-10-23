import datetime
import logging
import pandas_market_calendars
import polygon

from tau.core import Signal, HistoricNetworkScheduler, MutableSignal, SignalGenerator

from serenity.marketdata.api import MarketdataService, Trade
from serenity.model.exchange import ExchangeInstrument
from serenity.trading.api import Side


class PolygonHistoricEquityMarketdataService(MarketdataService):
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

            class TradeGenerator(SignalGenerator):
                def __init__(self, mds):
                    self.mds = mds

                def generate(self, scheduler: HistoricNetworkScheduler):
                    calendar = pandas_market_calendars.get_calendar(
                        instrument.get_exchange().get_exchange_calendar())
                    tz = instrument.get_exchange().get_exchange_tz()
                    exch_schedule_df = calendar.schedule(self.mds.start_date, self.mds.end_date, tz)
                    for row in exch_schedule_df.itertuples():
                        load_date = row[0].to_pydatetime().date()
                        market_open = int(row[1].to_pydatetime().timestamp() * 1000 * 1000 * 1000)
                        market_close = int(row[2].to_pydatetime().timestamp() * 1000 * 1000 * 1000)

                        timestamp = market_open
                        self.mds.logger.info(f'Starting historic trade download from timestamp={timestamp}')
                        while timestamp < market_close:
                            resp = self.mds.client.historic_trades_v2(trade_symbol, load_date, timestamp=timestamp)

                            for v2_trade in resp.results:
                                at_time = int(v2_trade['t'] / 1000 / 1000)
                                sequence = v2_trade['q']
                                trade_id = v2_trade['i']
                                qty = v2_trade['s']
                                price = v2_trade['p']

                                trade = Trade(instrument, sequence, trade_id, Side.UNKNOWN, qty, price)
                                self.mds.scheduler.schedule_update_at(trades, trade, at_time)

                            last_trade = resp.results[-1]
                            timestamp = last_trade['t']

            self.scheduler.add_generator(TradeGenerator(self))

        return trades

    def _maybe_notify_subscription(self, instrument: ExchangeInstrument):
        if instrument not in self.all_subscribed:
            self.scheduler.schedule_update(self.subscribed_instruments, instrument)
            self.all_subscribed.add(instrument)
