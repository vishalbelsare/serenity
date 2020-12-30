import datetime

from typing import List, Optional

import pandas as pd
import pandas_market_calendars as mcal
import pytz

from money import Money
from tau.core import NetworkScheduler, Event, HistoricNetworkScheduler

from serenity.strategy.api import DividendPolicy, Dividend, Portfolio, TradableUniverse, Tradable, PricingContext, \
    StrategyContext, PriceField, RebalanceContext, DividendContext, TradingContext, TradingCostCalculator, \
    MarketScheduleProvider, MarketSchedule, RebalanceSchedule
from serenity.trading.api import Side


class DefaultRebalanceContext(RebalanceContext):
    def __init__(self, scheduler: NetworkScheduler, portfolio: Portfolio, tradable_universe: TradableUniverse,
                 pricing_ctx: PricingContext, dividend_ctx: DividendContext, trading_ctx: TradingContext):
        super().__init__(portfolio, tradable_universe, pricing_ctx, dividend_ctx, trading_ctx)
        self.scheduler = scheduler

    def get_rebalance_time(self) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(int(self.scheduler.get_time() / 1000.0))


class FixedTradableUniverse(TradableUniverse):
    def __init__(self, tradables: List[Tradable]):
        self.tradables = tradables
        self.index = {tradable.get_ticker_id(): tradable for tradable in tradables}

    def lookup(self, ticker_id: int) -> Tradable:
        return self.index[ticker_id]

    def all(self) -> List[Tradable]:
        return self.tradables


class TradableUniversePricingContext(PricingContext):
    def __init__(self, ctx: StrategyContext, universe: TradableUniverse, delegate: PricingContext,
                 price_field: PriceField):
        self.ctx = ctx
        self.universe = universe
        self.delegate = delegate
        self.price_field = price_field
        self.price_histories = None
        self._preload()

    def price(self, tradable: Tradable, price_date: datetime.date, price_field: PriceField) -> Money:
        assert self.price_field == price_field
        return Money(self.price_histories.loc[price_date][[tradable.get_ticker_id()]].squeeze(), currency='USD')

    def get_price_history(self, tradables: List[Tradable], start_date: datetime.date, end_date: datetime.date,
                          price_field: PriceField) -> pd.DataFrame:
        return self.price_histories.loc[(self.price_histories.index >= start_date)
                                        & (self.price_histories.index <= end_date),
                                        [tradable.get_ticker_id() for tradable in tradables]]

    def _preload(self):
        tradables = self.universe.all()
        self.price_histories = self.delegate.get_price_history(tradables, self.ctx.get_start_time(),
                                                               self.ctx.get_end_time(), self.price_field)


class TradableUniverseDividendContext(DividendContext):
    def __init__(self, ctx: StrategyContext, universe: TradableUniverse, delegate: DividendContext):
        self.ctx = ctx
        self.universe = universe
        self.delegate = delegate
        self.div_streams = None
        self._preload()

    def get_dividend(self, tradable: Tradable, payment_date: datetime.date) -> Optional[Dividend]:
        try:
            amount = Money(self.div_streams.loc[payment_date][[tradable.get_ticker_id()]].squeeze(), currency='USD')
            return Dividend(tradable, payment_date, amount)
        except KeyError:
            return None

    def get_dividend_streams(self, tradables: List[Tradable], start_date: datetime.date,
                             end_date: datetime.date) -> pd.DataFrame:
        return self.div_streams.loc[(self.div_streams.index >= start_date)
                                    & (self.div_streams.index <= end_date),
                                    [tradable.get_ticker_id() for tradable in tradables]]

    def _preload(self):
        tradables = self.universe.all()
        self.div_streams = self.delegate.get_dividend_streams(tradables, self.ctx.get_start_time().date(),
                                                              self.ctx.get_end_time().date())


class NullDividendPolicy(DividendPolicy):
    def apply(self, div: Dividend, pf: Portfolio):
        pass


class ReinvestTradableDividendPolicy(DividendPolicy):
    def apply(self, div: Dividend, pf: Portfolio):
        pass


class ReinvestPortfolioDividendPolicy(DividendPolicy):
    def apply(self, div: Dividend, pf: Portfolio):
        pass


class AccumulateCashDividendPolicy(DividendPolicy):
    def apply(self, div: Dividend, pf: Portfolio):
        for account in pf.get_accounts():
            for position in account.get_positions():
                if position.get_tradable().get_ticker_id() == div.get_tradable().get_ticker_id():
                    qty = position.get_qty()
                    amt_paid = Money(qty * div.get_amount().amount, div.get_amount().currency)
                    account.get_cash_balance().deposit(amt_paid)


class ZeroCommissionTradingCostCalculator(TradingCostCalculator):
    def __init__(self, ccy: str = 'USD'):
        self.ccy = ccy

    def get_trading_cost_per_qty(self, side: Side, tradable: Tradable):
        return Money(0, self.ccy)


class PandasMarketCalendarMarketScheduleProvider(MarketScheduleProvider):
    def __init__(self, scheduler: HistoricNetworkScheduler, local_tz: str):
        self.scheduler = scheduler
        self.local_tz = local_tz

    def get_market_schedule(self, market: str) -> MarketSchedule:
        tz = pytz.timezone(self.local_tz)
        exch_calendar = mcal.get_calendar(market)
        schedule_df = exch_calendar.schedule(
            datetime.datetime.fromtimestamp(self.scheduler.get_start_time() / 1000.0, tz=tz),
            datetime.datetime.fromtimestamp(self.scheduler.get_end_time() / 1000.0, tz=tz))

        class NullEvent(Event):
            def on_activate(self) -> bool:
                return True

        market_open = NullEvent()
        market_close = NullEvent()
        for index, row in schedule_df.iterrows():
            market_open_dt = row['market_open'].astimezone(tz)
            market_close_dt = row['market_close'].astimezone(tz)

            self.scheduler.get_network().attach(market_open)
            self.scheduler.get_network().attach(market_close)
            self.scheduler.schedule_event_at(market_open, int(market_open_dt.timestamp() * 1000))
            self.scheduler.schedule_event_at(market_close, int(market_close_dt.timestamp() * 1000))

        return MarketSchedule(market_open, market_close)


class DailyRebalanceOnCloseSchedule(RebalanceSchedule):
    def get_rebalance_event(self, universe: TradableUniverse, msp: MarketScheduleProvider) -> Event:
        all_markets = set([tradable.get_market() for tradable in universe.all()])
        assert len(all_markets) == 1
        return msp.get_market_schedule(all_markets.pop()).get_market_close_event()
