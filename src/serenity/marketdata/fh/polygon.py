from tau.core import Signal

from serenity.marketdata.api import MarketdataService
from serenity.model.exchange import ExchangeInstrument


class PolygonMarketdataService(MarketdataService):
    def get_subscribed_instruments(self) -> Signal:
        pass

    def get_order_book_events(self, instrument: ExchangeInstrument) -> Signal:
        pass

    def get_order_books(self, instrument: ExchangeInstrument) -> Signal:
        pass

    def get_trades(self, instrument: ExchangeInstrument) -> Signal:
        pass
