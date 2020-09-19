import logging
from datetime import timedelta

from tau.core import Event
from tau.signal import Map, BufferWithTime

from serenity.algo import Strategy, StrategyContext
from serenity.signal.indicators import ComputeBollingerBands
from serenity.signal.marketdata import ComputeOHLC


class BollingerBandsStrategy1(Strategy):
    """
    An example strategy that uses Bollinger Band crossing as a signal to buy or sell.
    """

    logger = logging.getLogger(__name__)

    def init(self, ctx: StrategyContext):
        scheduler = ctx.get_scheduler()
        network = scheduler.get_network()

        window = int(ctx.getenv('BBANDS_WINDOW'))
        num_std = int(ctx.getenv('BBANDS_NUM_STD'))
        exchange_code, instrument_code = ctx.getenv('TRADING_INSTRUMENT').split('/')
        instrument = ctx.get_instrument_cache().get_exchange_instrument(exchange_code, instrument_code)
        trades = ctx.get_marketdata_service().get_trades(instrument)
        trades_5m = BufferWithTime(scheduler, trades, timedelta(minutes=5))
        prices = ComputeOHLC(network, trades_5m)
        close_prices = Map(network, prices, lambda x: x.close_px)
        bbands = ComputeBollingerBands(network, close_prices, window, num_std)

        class BollingerTrader(Event):
            def __init__(self, strategy: BollingerBandsStrategy1):
                self.strategy = strategy
                self.last_entry = 0
                self.last_exit = 0
                self.cum_pnl = 0
                self.has_position = False

            def on_activate(self) -> bool:
                if not self.has_position and close_prices.get_value() < bbands.get_value().lower:
                    self.last_entry = close_prices.get_value()
                    self.has_position = True
                    self.strategy.logger.info(f'Close below lower Bollinger band, entering long position at '
                                              f'{close_prices.get_value()}')

                elif self.has_position and close_prices.get_value() > bbands.get_value().upper:
                    self.cum_pnl = self.cum_pnl + close_prices.get_value() - self.last_entry
                    self.has_position = False
                    self.strategy.logger.info(f'Close above upper Bollinger band, exiting long position at '
                                              f'{close_prices.get_value()}, cum PnL={self.cum_pnl}')
                return False

        network.connect(bbands, BollingerTrader(self))
