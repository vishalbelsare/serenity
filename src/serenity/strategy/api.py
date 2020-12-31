import datetime
from abc import ABC, abstractmethod
from decimal import Decimal
from enum import Enum, auto
from typing import Optional, List

import pandas as pd
from money import Money
from tau.core import Event, NetworkScheduler

from serenity.trading.api import Side


class Tradable(ABC):
    @abstractmethod
    def get_ticker_id(self) -> int:
        pass

    @abstractmethod
    def get_symbol(self) -> str:
        pass

    @abstractmethod
    def get_market(self) -> str:
        pass

    @abstractmethod
    def get_name(self) -> str:
        pass

    @abstractmethod
    def get_currency(self) -> str:
        pass


class TradableUniverse(ABC):
    @abstractmethod
    def lookup(self, ticker_id: int) -> Tradable:
        pass

    @abstractmethod
    def all(self) -> List[Tradable]:
        pass


class PriceField(Enum):
    OPEN = auto()
    CLOSE = auto()
    BID = auto()
    ASK = auto()


class PricingContext(ABC):
    @abstractmethod
    def price(self, tradable: Tradable, price_date: datetime.date, price_field: PriceField) -> Money:
        pass

    @abstractmethod
    def get_price_history(self, tradables: List[Tradable], start_date: datetime.date,
                          end_date: datetime.date, price_field: PriceField) -> pd.DataFrame:
        pass


class Transaction:
    def __init__(self, side: Side, tradable: Tradable, qty: Decimal, executed_qty: Decimal,
                 executed_price: Money, executed_cost: Money):
        self.side = side
        self.tradable = tradable
        self.qty = qty
        self.executed_qty = executed_qty
        self.executed_price = executed_price
        self.executed_cost = executed_cost
        self.executed_notional = Money(self.executed_qty * self.executed_price.amount, self.executed_price.currency)

    def get_side(self) -> Side:
        return self.side

    def get_tradable(self) -> Tradable:
        return self.tradable

    def get_qty(self) -> Decimal:
        return self.qty

    def get_executed_qty(self) -> Decimal:
        return self.executed_qty

    def get_executed_price(self) -> Money:
        return self.executed_price

    def get_executed_cost(self) -> Money:
        return self.executed_cost

    def get_executed_notional(self) -> Money:
        return self.executed_notional

    def __str__(self):
        return f'{self.side} {self.qty} @ {self.executed_price} (cost={self.executed_cost})'


class CashBalance:
    def __init__(self, ccy: str):
        self.balance = Money(0, ccy)

    def get_balance(self) -> Money:
        return self.balance

    def deposit(self, money: Money):
        self.balance += money

    def withdraw(self, money: Money):
        self.balance -= money


class Position:
    def __init__(self, tradable: Tradable):
        self.tradable = tradable
        self.qty = Decimal(0.0)
        self.px = Money(0.0, tradable.get_currency())

    def get_tradable(self) -> Tradable:
        return self.tradable

    def get_qty(self) -> Decimal:
        return self.qty

    def get_notional(self) -> Money:
        return Money(self.px.amount * self.qty, self.px.currency)

    def apply(self, tx: Transaction, cash: CashBalance):
        if tx.get_side() == Side.BUY:
            self.qty += tx.get_qty()
            cash.withdraw(tx.get_executed_cost())
            cash.withdraw(tx.get_executed_notional())
        elif tx.get_side() == Side.SELL:
            self.qty -= tx.get_qty()
            cash.withdraw(tx.get_executed_cost())
            cash.deposit(tx.get_executed_notional())
        else:
            raise ValueError(f'Side not handled: {tx.get_side()}')

    def mark(self, mark_date: datetime.date, pricing_ctx: PricingContext):
        self.px = pricing_ctx.price(self.tradable, mark_date, PriceField.CLOSE)


class Account:
    def __init__(self, name: str, ccy: str):
        self.name = name
        self.cash_balance = CashBalance(ccy)
        self.positions = {}

    def get_name(self) -> str:
        return self.name

    def get_cash_balance(self) -> CashBalance:
        return self.cash_balance

    def get_position(self, tradable: Tradable) -> Position:
        position = self.positions.get(tradable.get_ticker_id(), None)
        if position is None:
            position = Position(tradable)
            self.positions[tradable.get_ticker_id()] = position
        return position

    def get_positions(self) -> List[Position]:
        return list(self.positions.values())

    def apply(self, tx: Transaction):
        self.get_position(tx.get_tradable()).apply(tx, self.get_cash_balance())

    def mark(self, mark_date: datetime.date, pricing_ctx: PricingContext):
        for position in self.positions.values():
            position.mark(mark_date, pricing_ctx)


class Portfolio:
    def __init__(self, account_names: List[str], ccy: str):
        self.accounts = {account_name: Account(account_name, ccy) for account_name in account_names}
        self.ccy = ccy

    def get_account(self, name: str) -> Account:
        return self.accounts[name]

    def get_accounts(self) -> List[Account]:
        return list(self.accounts.values())

    def mark(self, mark_date: datetime.date, pricing_ctx: PricingContext):
        for account in self.accounts.values():
            account.mark(mark_date, pricing_ctx)


class Dividend:
    def __init__(self, tradable: Tradable, payment_date: datetime.date, amount: Money):
        self.tradable = tradable
        self.payment_date = payment_date
        self.amount = amount

    def get_tradable(self) -> Tradable:
        return self.tradable

    def get_payment_date(self) -> datetime.date:
        return self.payment_date

    def get_amount(self) -> Money:
        return self.amount

    def __str__(self) -> str:
        return f'{self.amount} paid on {self.payment_date}'


class DividendPolicy(ABC):
    @abstractmethod
    def apply(self, div: Dividend, pf: Portfolio):
        pass


class TradingCostCalculator(ABC):
    @abstractmethod
    def get_trading_cost_per_qty(self, side: Side, tradable: Tradable):
        pass


class TradingContext(ABC):
    def __init__(self, tc_calc: TradingCostCalculator):
        self.tc_calc = tc_calc

    def get_trading_cost_per_qty(self, side: Side, tradable: Tradable):
        return self.tc_calc.get_trading_cost_per_qty(side, tradable)

    @abstractmethod
    def buy(self, tradable: Tradable, qty: Decimal, limit_px: Optional[Decimal] = None) -> Transaction:
        pass

    @abstractmethod
    def sell(self, tradable: Tradable, qty: Decimal, limit_px: Optional[Decimal] = None) -> Transaction:
        pass


class DividendContext(ABC):
    @abstractmethod
    def get_dividend(self, tradable: Tradable, payment_date: datetime.date) -> Optional[Dividend]:
        pass

    @abstractmethod
    def get_dividend_streams(self, tradables: List[Tradable], start_date: datetime.date,
                             end_date: datetime.date) -> pd.DataFrame:
        pass


class StrategyContext(ABC):
    @abstractmethod
    def get_start_time(self) -> datetime.datetime:
        pass

    @abstractmethod
    def get_end_time(self) -> Optional[datetime.datetime]:
        pass

    @abstractmethod
    def get_configuration(self) -> dict:
        pass


class Strategy(ABC):
    @abstractmethod
    def init(self, ctx: StrategyContext):
        pass


class RebalanceContext(ABC):
    def __init__(self, portfolio: Portfolio, tradable_universe: TradableUniverse,
                 pricing_ctx: PricingContext, dividend_ctx: DividendContext,
                 trading_ctx: TradingContext):
        self.portfolio = portfolio
        self.tradable_universe = tradable_universe
        self.pricing_ctx = pricing_ctx
        self.dividend_ctx = dividend_ctx
        self.trading_ctx = trading_ctx

    @abstractmethod
    def get_rebalance_time(self) -> datetime.datetime:
        pass

    def get_portfolio(self) -> Portfolio:
        return self.portfolio

    def get_tradable_universe(self) -> TradableUniverse:
        return self.tradable_universe

    def get_pricing_ctx(self) -> PricingContext:
        return self.pricing_ctx

    def get_dividend_ctx(self) -> DividendContext:
        return self.dividend_ctx

    def get_trading_ctx(self) -> TradingContext:
        return self.trading_ctx


class MarketSchedule:
    def __init__(self, schedule_df: pd.DataFrame, market_open_event: Event, market_close_event: Event):
        self.schedule_df = schedule_df
        self.market_open_event = market_open_event
        self.market_close_event = market_close_event

    def get_schedule(self) -> pd.DataFrame:
        return self.schedule_df

    def get_market_open_event(self) -> Event:
        return self.market_open_event

    def get_market_close_event(self) -> Event:
        return self.market_close_event


class MarketScheduleProvider:
    @abstractmethod
    def get_market_schedule(self, market: str) -> MarketSchedule:
        pass


class RebalanceSchedule(ABC):
    @abstractmethod
    def get_rebalance_event(self) -> Event:
        pass


class InvestmentStrategy(Strategy):
    @abstractmethod
    def get_initial_portfolio(self) -> Portfolio:
        pass

    @abstractmethod
    def get_tradable_universe(self, base_universe: TradableUniverse):
        pass

    @abstractmethod
    def get_rebalance_schedule(self, scheduler: NetworkScheduler, universe: TradableUniverse,
                               msp: MarketScheduleProvider) -> RebalanceSchedule:
        pass

    @abstractmethod
    def get_dividend_policy(self, trading_ctx: TradingContext, pricing_ctx: PricingContext) -> DividendPolicy:
        pass

    @abstractmethod
    def rebalance(self, rebalance_ctx: RebalanceContext):
        pass
