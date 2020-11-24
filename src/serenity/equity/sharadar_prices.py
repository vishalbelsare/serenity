from sqlalchemy import Column, Integer, Date, ForeignKey
from sqlalchemy.dialects.postgresql import MONEY
from sqlalchemy.orm import relationship

from serenity.equity.sharadar_api import Base


class EquityPrice(Base):
    __tablename__ = 'equity_price'

    equity_price_id = Column(Integer, primary_key=True)
    ticker_id = Column(Integer, ForeignKey('ticker.ticker_id'))
    ticker = relationship('Ticker')
    date = Column(Date, name='price_date')
    open_px = Column(MONEY, name='open')
    high_px = Column(MONEY, name='high')
    low_px = Column(MONEY, name='low')
    close_px = Column(MONEY, name='close')
    volume = Column(Integer)
    dividends = Column(MONEY)
    close_unadj = Column(MONEY)
    last_updated = Column(Date)


