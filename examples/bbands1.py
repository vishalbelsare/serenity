import logging
from datetime import timedelta, datetime

from tau.core import Event, NetworkScheduler
from tau.signal import Map, BufferWithTime

from serenity.algo import Strategy, StrategyContext
from serenity.signal.indicators import ComputeBollingerBands
from serenity.signal.marketdata import ComputeOHLC
from serenity.trading import OrderPlacerService, Side, OrderStatus


class BollingerBandsStrategy1(Strategy):
    """
    An example strategy that uses Bollinger Band crossing as a signal to buy or sell.
    """

    logger = logging.getLogger(__name__)

    def init(self, ctx: StrategyContext):
        scheduler = ctx.get_scheduler()
        network = scheduler.get_network()

        contract_qty = int(ctx.getenv('BBANDS_QTY', 1))
        window = int(ctx.getenv('BBANDS_WINDOW'))
        num_std = int(ctx.getenv('BBANDS_NUM_STD'))
        stop_std = int(ctx.getenv('BBANDS_STOP_STD'))
        exchange_code, instrument_code = ctx.getenv('TRADING_INSTRUMENT').split('/')
        instrument = ctx.get_instrument_cache().get_exchange_instrument(exchange_code, instrument_code)
        trades = ctx.get_marketdata_service().get_trades(instrument)
        trades_5m = BufferWithTime(scheduler, trades, timedelta(minutes=5))
        prices = ComputeOHLC(network, trades_5m)
        close_prices = Map(network, prices, lambda x: x.close_px)
        bbands = ComputeBollingerBands(network, close_prices, window, num_std)

        op_service = ctx.get_order_placer_service()
        exchange_id = ctx.getenv('EXCHANGE_ID', 'phemex')
        exchange_instance = ctx.getenv('EXCHANGE_INSTANCE', 'prod')
        op_uri = f'{exchange_id}:{exchange_instance}'

        class BollingerTrader(Event):
            # noinspection PyShadowingNames
            def __init__(self, scheduler: NetworkScheduler, op_service: OrderPlacerService,
                         strategy: BollingerBandsStrategy1):
                self.scheduler = scheduler
                self.op = op_service.get_order_placer(f'{op_uri}')
                self.strategy = strategy
                self.last_entry = 0
                self.last_exit = 0
                self.cum_pnl = 0
                self.position = 0
                self.stop = None

                self.scheduler.get_network().connect(self.op.get_order_events(), self)

            def on_activate(self) -> bool:
                if self.scheduler.get_network().has_activated(self.op.get_order_events()):
                    order_event = self.op.get_order_events().get_value()
                    if order_event.get_order_status() == OrderStatus.FILLED:
                        order_type = 'stop order' if order_event.get_order_id() == self.stop.order_id \
                            else 'market order'
                        self.strategy.logger.info(f'Received fill event for {order_type}: {order_event}')
                        if self.position == 0:
                            self.last_entry = order_event.get_last_px()
                            self.position = self.position + order_event.get_last_qty()
                        else:
                            self.cum_pnl = (order_event.get_last_px() - self.last_entry) * \
                                           (self.position / self.last_entry)
                            self.position = self.position - order_event.get_last_qty()
                            self.strategy.logger.info(f'Cumulative P&L: {self.cum_pnl}')

                    self.cum_pnl = self.cum_pnl + close_prices.get_value() - self.last_entry
                elif self.position == 0 and close_prices.get_value() < bbands.get_value().lower:
                    self.strategy.logger.info(f'Close below lower Bollinger band, entering long position at '
                                              f'{datetime.fromtimestamp(self.scheduler.get_time() / 1000.0)}')

                    stop_px = close_prices.get_value() - ((bbands.get_value().sma - bbands.get_value().lower) *
                                                          (stop_std / num_std))
                    order = self.op.get_order_factory().create_market_order(Side.BUY, contract_qty, instrument)
                    self.stop = self.op.get_order_factory().create_stop_order(Side.SELL, contract_qty, stop_px,
                                                                              instrument)

                    self.strategy.logger.info(f'Submitting orders: last_px = {close_prices.get_value()}, '
                                              f'stop_px = {stop_px}')

                    self.op.submit(order)
                    self.op.submit(self.stop)
                elif self.position > 0 and close_prices.get_value() > bbands.get_value().upper:
                    self.strategy.logger.info(f'Close above upper Bollinger band, exiting long position at '
                                              f'{datetime.fromtimestamp(self.scheduler.get_time() / 1000.0)}')
                    order = self.op.get_order_factory().create_market_order(Side.SELL, contract_qty, instrument)
                    self.op.submit(order)
                    self.op.cancel(self.stop)

                return False

        network.connect(bbands, BollingerTrader(scheduler, op_service, self))
