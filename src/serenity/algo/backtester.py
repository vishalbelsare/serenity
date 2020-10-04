import importlib
import logging
import sys
import time
from datetime import datetime

import fire
import yaml
from tau.core import HistoricNetworkScheduler
from tau.event import Do

from serenity.algo.api import StrategyContext
from serenity.analytics.api import HDF5DataCaptureService, Mode
from serenity.db.api import connect_serenity_db, InstrumentCache, TypeCodeCache
from serenity.marketdata.historic import HistoricMarketdataService
from serenity.position.api import PositionService, NullExchangePositionService
from serenity.trading.oms import OrderManagerService, OrderPlacerService
from serenity.trading.connector.simulator import AutoFillOrderPlacer
from serenity.utils import init_logging, Environment


class AlgoBacktester:
    """"
    Algorithmic trading strategy backtester.
    """

    logger = logging.getLogger(__name__)

    def __init__(self, config_path: str, strategy_dir: str, start_time_millis: int, end_time_millis: int):
        sys.path.append(strategy_dir)

        with open(config_path, 'r') as config_yaml:
            logger = logging.getLogger(__name__)

            logger.info('Serenity backtester starting up')
            config = yaml.safe_load(config_yaml)
            api_version = config['api-version']
            if api_version != 'v1Beta':
                raise ValueError(f'Unsupported API version: {api_version}')

            self.bt_env = Environment(config['environment'])
            exchange_id = self.bt_env.getenv('EXCHANGE_ID', 'autofill')
            instance_id = self.bt_env.getenv('EXCHANGE_INSTANCE', 'prod')
            account = self.bt_env.getenv('EXCHANGE_ACCOUNT', 'Main')

            self.logger.info('Connecting to Serenity database')
            conn = connect_serenity_db()
            conn.autocommit = True
            cur = conn.cursor()

            self.scheduler = HistoricNetworkScheduler(start_time_millis, end_time_millis)
            instrument_cache = InstrumentCache(cur, TypeCodeCache(cur))
            instruments_to_cache_txt = self.bt_env.getenv('INSTRUMENTS_TO_CACHE')
            instruments_to_cache_list = instruments_to_cache_txt.split(',')
            instruments_to_cache = []
            for instrument in instruments_to_cache_list:
                exchange, symbol = instrument.split('/')
                instruments_to_cache.append(instrument_cache.get_exchange_instrument(exchange, symbol))
            oms = OrderManagerService(self.scheduler)
            md_service = HistoricMarketdataService(self.scheduler, instruments_to_cache,
                                                   self.bt_env.getenv('AZURE_CONNECT_STR'))
            op_service = OrderPlacerService(self.scheduler, oms)
            op_service.register_order_placer(f'{exchange_id}:{instance_id}',
                                             AutoFillOrderPlacer(self.scheduler, oms, md_service, account))

            xps = NullExchangePositionService(self.scheduler)

            extra_outputs_txt = self.bt_env.getenv('EXTRA_OUTPUTS', None)
            if extra_outputs_txt is None:
                extra_outputs = []
            else:
                extra_outputs = extra_outputs_txt.split(',')
            self.dcs = HDF5DataCaptureService(Mode.BACKTEST, self.scheduler, extra_outputs)

            # wire up orders and fills from OMS
            Do(self.scheduler.get_network(), oms.get_orders(),
               lambda: self.dcs.capture_order(oms.get_orders().get_value()))
            Do(self.scheduler.get_network(), oms.get_order_events(),
               lambda: self.dcs.capture_fill(oms.get_order_events().get_value()))

            self.strategies = []
            self.strategy_names = []
            for strategy in config['strategies']:
                strategy_name = strategy['name']
                self.strategy_names.append(strategy_name)
                self.logger.info(f'Loading strategy: {strategy_name}')
                module = strategy['module']
                strategy_class = strategy['strategy-class']
                env = Environment(strategy['environment'], parent=self.bt_env)

                module = importlib.import_module(module)
                klass = getattr(module, strategy_class)
                strategy_instance = klass()
                ctx = StrategyContext(self.scheduler, instrument_cache, md_service, op_service,
                                      PositionService(self.scheduler, oms), xps, self.dcs, env.values)
                strategy_instance.init(ctx)
                strategy_instance.start()

    def run(self):
        self.scheduler.run()

        # store output after historic run completes
        for strategy_name in self.strategy_names:
            snapshot_id = self.dcs.store_snapshot(strategy_name)
            self.logger.info(f'stored snapshot: {snapshot_id}')


def main(config_path: str, start_time: str = None, end_time: str = None, strategy_dir: str = '.'):
    init_logging()

    timestamp_fmt = '%Y-%m-%dT%H:%M:%S'
    start_time_millis = int(time.mktime(datetime.strptime(start_time, timestamp_fmt).timetuple()) * 1000)
    end_time_millis = int(time.mktime(datetime.strptime(end_time, timestamp_fmt).timetuple()) * 1000)

    engine = AlgoBacktester(config_path, strategy_dir, start_time_millis, end_time_millis)
    engine.run()


if __name__ == '__main__':
    fire.Fire(main)
