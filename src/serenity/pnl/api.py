from abc import ABC

from tau.core import Signal

from serenity.model.exchange import ExchangeInstrument


class MarkService(ABC):
    """
    Service providing a "fair" mark price for P&L calculation purposes.
    """
    def get_marks(self, instrument: ExchangeInstrument) -> Signal:
        pass
