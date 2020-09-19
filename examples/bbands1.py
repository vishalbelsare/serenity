import logging
from datetime import timedelta, datetime

from tau.event import Do
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
        Do(network, trades, lambda: self.logger.info(f'Trade printed: {trades.get_value()}, '
                                                     f'time={datetime.fromtimestamp(scheduler.get_time() / 1000.0)}'))
        trades_5m = BufferWithTime(scheduler, trades, timedelta(minutes=5))
        prices = ComputeOHLC(network, trades_5m)
        Do(network, prices, lambda: self.logger.info(f'Trade bin closed: close px={prices.get_value().close_px}, ' 
                                                     f'time={datetime.fromtimestamp(scheduler.get_time() / 1000.0)}'))
        close_prices = Map(network, prices, lambda x: x.close_px)
        bbands = ComputeBollingerBands(network, close_prices, window, num_std)
        Do(network, bbands, lambda: self.logger.info(f'Bollinger Bands updated: {bbands.get_value()}'))
