from uuid import uuid1

from tau.core import Signal, NetworkScheduler, MutableSignal, Event

from serenity.marketdata import MarketdataService
from serenity.trading import OrderPlacer, Order, ExecutionReport, ExecType, OrderStatus, CancelReject, OrderFactory, \
    Side, StopOrder, LimitOrder, MarketOrder, OrderManagerService


class AutoFillOrderPlacer(OrderPlacer):
    def __init__(self, scheduler: NetworkScheduler, oms: OrderManagerService, mds: MarketdataService, account: str):
        super().__init__(OrderFactory(account))
        self.scheduler = scheduler
        self.oms = oms
        self.mds = mds

        self.order_events = MutableSignal()
        self.scheduler.get_network().attach(self.order_events)

        self.orders = {}

    def get_order_events(self) -> Signal:
        return self.order_events

    def submit(self, order: Order):
        order_id = str(uuid1())
        order.set_order_id(order_id)

        pending_new_rpt = ExecutionReport(order_id, order.get_cl_ord_id(), str(uuid1()), ExecType.PENDING_NEW,
                                          OrderStatus.PENDING_NEW, 0.0, order.get_qty(), 0.0, 0.0)
        self.scheduler.schedule_update(self.order_events, pending_new_rpt)

        new_rpt = ExecutionReport(order_id, order.get_cl_ord_id(), str(uuid1()), ExecType.NEW,
                                  OrderStatus.NEW, 0.0, order.get_qty(), 0.0, 0.0)
        self.scheduler.schedule_update(self.order_events, new_rpt)

        class FillScheduler(Event):
            # noinspection PyShadowingNames
            def __init__(self, order_placer: AutoFillOrderPlacer, trades: Signal, order_books: Signal):
                self.order_placer = order_placer
                self.trades = trades
                self.order_books = order_books
                self.fired = False

            def on_activate(self) -> bool:
                if self.fired or order.get_order_id() not in self.order_placer.orders:
                    return False

                if self.order_placer.scheduler.get_network().has_activated(self.trades):
                    if isinstance(order, StopOrder):
                        if self.trades.get_value().get_price() <= order.get_stop_px():
                            self.__apply_fill_at_market_px(order)
                            self.fired = True
                elif self.order_placer.scheduler.get_network().has_activated(self.order_books):
                    if isinstance(order, LimitOrder) or isinstance(order, MarketOrder):
                        self.__apply_fill_at_market_px(order)
                        self.fired = True

                return True

            # noinspection PyShadowingNames
            def __apply_fill_at_market_px(self, order: Order):
                if order.get_side() == Side.BUY:
                    fill_px = self.order_books.get_value().get_best_ask().get_px()
                else:
                    fill_px = self.order_books.get_value().get_best_bid().get_px()
                fill_rpt = ExecutionReport(order_id, order.get_cl_ord_id(), str(uuid1()), ExecType.TRADE,
                                           OrderStatus.FILLED, order.get_qty(), 0.0, fill_px, order.get_qty())
                self.order_placer.scheduler.schedule_update(self.order_placer.order_events, fill_rpt)
                self.fired = True

        trades = self.mds.get_trades(order.get_instrument())
        order_books = self.mds.get_order_books(order.get_instrument())
        self.orders[order_id] = order

        fill_scheduler = FillScheduler(self, trades, order_books)
        self.scheduler.get_network().connect(trades, fill_scheduler)
        self.scheduler.get_network().connect(order_books, fill_scheduler)

    def cancel(self, order):
        if isinstance(order, StopOrder):
            del self.orders[order.get_order_id()]
        else:
            reject = CancelReject(order.get_cl_ord_id(), order.get_cl_ord_id(), 'Already fully filled')
            self.scheduler.schedule_update(self.order_events, reject)
