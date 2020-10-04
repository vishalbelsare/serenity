import logging
from enum import Enum, auto

import pandas as pd

from datetime import timedelta, datetime

from tau.core import Event, NetworkScheduler, Signal, Network
from tau.event import Do
from tau.signal import Map, BufferWithTime, Function

from serenity.algo.api import Strategy, StrategyContext
from serenity.signal.indicators import ComputeBollingerBands
from serenity.signal.marketdata import ComputeOHLC
from serenity.trading.api import Side, OrderStatus, ExecutionReport, Reject
from serenity.trading.oms import OrderPlacerService


class ComputeTradeFlowImbalanceSignal(Function):
    def __init__(self, network: Network, trades: Signal):
        super().__init__(network, [trades])
        self.trades = trades
        self.cum_buy_volume = 0
        self.cum_sell_volume = 0

    def _call(self):
        last_trade = self.trades.get_value()
        if last_trade.get_side() == Side.BUY:
            self.cum_buy_volume = last_trade.get_qty()
        else:
            self.cum_sell_volume = last_trade.get_qty()
        self._update(self.cum_buy_volume - self.cum_sell_volume)


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
        bin_minutes = int(ctx.getenv('BBANDS_BIN_MINUTES', 5))
        exchange_code, instrument_code = ctx.getenv('TRADING_INSTRUMENT').split('/')
        trade_flow_qty = int(ctx.getenv('TRADE_FLOW_REVERSAL_QTY', 10_000))
        instrument = ctx.get_instrument_cache().get_exchange_instrument(exchange_code, instrument_code)
        trades = ctx.get_marketdata_service().get_trades(instrument)
        trades_5m = BufferWithTime(scheduler, trades, timedelta(minutes=bin_minutes))
        prices = ComputeOHLC(network, trades_5m)
        close_prices = Map(network, prices, lambda x: x.close_px)
        bbands = ComputeBollingerBands(network, close_prices, window, num_std)
        trade_flow = ComputeTradeFlowImbalanceSignal(network, trades)

        op_service = ctx.get_order_placer_service()
        oms = op_service.get_order_manager_service()
        dcs = ctx.get_data_capture_service()

        exchange_id = ctx.getenv('EXCHANGE_ID', 'phemex')
        exchange_instance = ctx.getenv('EXCHANGE_INSTANCE', 'prod')
        account = ctx.getenv('EXCHANGE_ACCOUNT')
        op_uri = f'{exchange_id}:{exchange_instance}'

        # subscribe to position updates and exchange position updates
        position = ctx.get_position_service().get_position(account, instrument)
        Do(scheduler.get_network(), position, lambda: self.logger.info(position.get_value()))

        exch_position = ctx.get_exchange_position_service().get_exchange_positions()
        Do(scheduler.get_network(), exch_position, lambda: self.logger.info(exch_position.get_value()))

        # capture trade flow and Bollinger Band data
        Do(scheduler.get_network(), trade_flow, lambda: dcs.capture('TradeFlows', {
            'time': pd.to_datetime(scheduler.get_time(), unit='ms'),
            'trade_flow': trade_flow.get_value()
        }))
        Do(scheduler.get_network(), bbands, lambda: dcs.capture('BollingerBands', {
            'time': pd.to_datetime(scheduler.get_time(), unit='ms'),
            'sma': bbands.get_value().sma,
            'upper': bbands.get_value().upper,
            'lower': bbands.get_value().lower
        }))

        # log basic marketdata
        Do(scheduler.get_network(), trades, lambda: self.logger.info(trades.get_value()))
        Do(scheduler.get_network(), prices, lambda: self.logger.info(prices.get_value()))

        class TraderState(Enum):
            GOING_LONG = auto()
            LONG = auto()
            FLAT = auto()

        # order placement logic
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
                self.stop = None
                self.volatility_pause = False
                self.trader_state = TraderState.FLAT

                self.scheduler.get_network().connect(oms.get_order_events(), self)
                self.scheduler.get_network().connect(position, self)
                self.scheduler.get_network().connect(trade_flow, self)

            def on_activate(self) -> bool:
                if self.scheduler.get_network().has_activated(oms.get_order_events()):
                    order_event = oms.get_order_events().get_value()
                    if isinstance(order_event, ExecutionReport) and order_event.is_fill():
                        order_type = 'stop order' if order_event.get_order_id() == self.stop.order_id \
                            else 'market order'
                        self.strategy.logger.info(f'Received fill event for {order_type}: {order_event}')
                        if self.trader_state == TraderState.LONG:
                            trade_pnl = (order_event.get_last_px() - self.last_entry) * \
                                        (position.get_value().get_qty() / self.last_entry)
                            self.cum_pnl = self.cum_pnl + trade_pnl
                            self.strategy.logger.info(f'Trade P&L={trade_pnl}; cumulative P&L={self.cum_pnl}')

                            dcs.capture('PnL', {
                                'time': pd.to_datetime(scheduler.get_time(), unit='ms'),
                                'trade_pnl': trade_pnl,
                                'cum_pnk': self.cum_pnl
                            })

                            if order_event.get_order_status() == OrderStatus.FILLED:
                                self.trader_state = TraderState.FLAT
                        elif self.trader_state == TraderState.GOING_LONG:
                            self.last_entry = order_event.get_last_px()
                            self.strategy.logger.info(f'Entered long position: entry price={self.last_entry}')
                            self.trader_state = TraderState.LONG
                    elif isinstance(order_event, Reject):
                        self.strategy.logger.error(f'Order rejected: {order_event.get_message()}')
                elif self.scheduler.get_network().has_activated(trade_flow):
                    if trade_flow.get_value() < (-1 * trade_flow_qty) and not self.volatility_pause:
                        self.volatility_pause = True
                        self.strategy.logger.info(f'Toggled volatility pause ON: {trade_flow.get_value()}')
                        return False
                    elif trade_flow.get_value() > trade_flow_qty and self.volatility_pause:
                        self.volatility_pause = False
                        self.strategy.logger.info(f'Toggled volatility pause OFF: {trade_flow.get_value()}')
                        return False
                elif self.trader_state == TraderState.FLAT and close_prices.get_value() < bbands.get_value().lower:
                    if self.volatility_pause:
                        self.strategy.logger.info(f'Net selling pressure while below BB lower bound; paused trading at '
                                                  f'{datetime.fromtimestamp(self.scheduler.get_time() / 1000.0)}')
                        return False

                    self.strategy.logger.info(f'Close below lower Bollinger band while rallying, enter long position '
                                              f'at {datetime.fromtimestamp(self.scheduler.get_time() / 1000.0)}')
                    self.trader_state = TraderState.GOING_LONG

                    stop_px = close_prices.get_value() - ((bbands.get_value().sma - bbands.get_value().lower) *
                                                          (stop_std / num_std))
                    order = self.op.get_order_factory().create_market_order(Side.BUY, contract_qty, instrument)
                    self.stop = self.op.get_order_factory().create_stop_order(Side.SELL, contract_qty, stop_px,
                                                                              instrument)

                    self.strategy.logger.info(f'Submitting orders: last_px = {close_prices.get_value()}, '
                                              f'stop_px = {stop_px}')

                    self.op.submit(order)
                    self.op.submit(self.stop)
                elif position.get_value().get_qty() > 0 and close_prices.get_value() > bbands.get_value().upper:
                    self.strategy.logger.info(f'Close above upper Bollinger band, exiting long position at '
                                              f'{datetime.fromtimestamp(self.scheduler.get_time() / 1000.0)}')

                    order = self.op.get_order_factory().create_market_order(Side.SELL, contract_qty, instrument)
                    self.op.submit(order)
                    self.op.cancel(self.stop)

                return False

        network.connect(bbands, BollingerTrader(scheduler, op_service, self))
