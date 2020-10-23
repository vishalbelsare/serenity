from datetime import datetime

from polygon.rest.models import HistoricTradesV2ApiResponse
from pytest_mock import MockFixture
from tau.core import HistoricNetworkScheduler

from serenity.marketdata.polygon import PolygonHistoricEquityMarketdataService
from serenity.model.exchange import ExchangeInstrument, Exchange, VenueType
from serenity.model.instrument import Instrument, InstrumentType
from serenity.utils import init_logging

init_logging()


def test_polygon_historic_equity_marketdata_service(mocker: MockFixture):
    start_millis = int(datetime.strptime('2020-10-08 00:00:00', '%Y-%m-%d %H:%M:%S').timestamp() * 1000)
    end_millis = int(datetime.strptime('2020-10-08 23:59:59', '%Y-%m-%d %H:%M:%S').timestamp() * 1000)

    scheduler = HistoricNetworkScheduler(start_millis, end_millis)

    # noinspection DuplicatedCode
    venue_type = VenueType(-1, 'EquityExchange')
    exch = Exchange(-1, venue_type, 'NGS', 'Nasdaq Global Select', 'NYSE', 'America/New_York')
    instr_type = InstrumentType(-1, 'ETF')
    instr = Instrument(-1, instr_type, 'SPY')
    xinstr = ExchangeInstrument(-1, exch, instr, 'SPY')

    mock_rest_client = mocker.patch('polygon.RESTClient').return_value
    hist_response = HistoricTradesV2ApiResponse()
    trade_print1 = {'t': 1602163800000000000, 'q': 0, 'i': 0, 's': 100, 'p': 342.78}
    trade_print2 = {'t': 1602187200000000000, 'q': 0, 'i': 0, 's': 200, 'p': 343.62}

    hist_response.results = [
        trade_print1,
        trade_print2
    ]
    mock_rest_client.historic_trades_v2.return_value = hist_response
    pmds = PolygonHistoricEquityMarketdataService(scheduler, '********')
    trades = pmds.get_trades(xinstr)
    scheduler.run()

    assert trades.get_value() is not None
