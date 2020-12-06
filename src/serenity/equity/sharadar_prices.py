import pandas as pd

from sqlalchemy import Column, Integer, Date, ForeignKey
from sqlalchemy.orm import relationship, Session

from serenity.equity.sharadar_api import Base, USD
from serenity.equity.sharadar_refdata import Ticker


class EquityPrice(Base):
    __tablename__ = 'equity_price'

    equity_price_id = Column(Integer, primary_key=True)
    ticker_id = Column(Integer, ForeignKey('ticker.ticker_id'))
    ticker = relationship('serenity.equity.sharadar_refdata.Ticker', lazy='joined')
    date = Column(Date, name='price_date')
    open_px = Column(USD, name='open')
    high_px = Column(USD, name='high')
    low_px = Column(USD, name='low')
    close_px = Column(USD, name='close')
    volume = Column(Integer)
    dividends = Column(USD)
    close_unadj = Column(USD)
    last_updated = Column(Date)


def get_equity_prices(session: Session, ticker: str) -> pd.DataFrame:
    results = session.query(EquityPrice) \
        .join(Ticker) \
        .filter(Ticker.ticker == ticker).all()
    df = pd.DataFrame([{
        'date': result.date,
        'open': pd.to_numeric(result.open_px.amount),
        'high': pd.to_numeric(result.high_px.amount),
        'low': pd.to_numeric(result.low_px.amount),
        'close': pd.to_numeric(result.close_px.amount),
        'volume': result.volume
    } for result in results])
    if df.empty:
        raise KeyError(f'no data for ticker: {ticker}')
    else:
        df.set_index('date', inplace=True)
    return df
