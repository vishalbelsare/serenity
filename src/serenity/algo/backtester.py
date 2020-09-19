import importlib
import logging
import sys
import time
from datetime import datetime

import fire
import yaml
from tau.core import HistoricNetworkScheduler

from serenity.algo import StrategyContext
from serenity.db import connect_serenity_db, InstrumentCache, TypeCodeCache
from serenity.marketdata.historic import HistoricMarketdataService
from serenity.trading import OrderPlacerService
from serenity.trading.connector.simulator import AutoFillOrderPlacer
from serenity.utils import init_logging, Environment


class AlgoBacktester:
    """"
    Algorithmic trading strategy backtester.
    """

    logger = logging.getLogger(__name__)

    def __init__(self, config_path: str, strategy_module: str, strategy_class: str, strategy_dir: str,
                 start_time_millis: int, end_time_millis: int):
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

            module = importlib.import_module(strategy_module)
            klass = getattr(module, strategy_class)
            strategy_instance = klass()

            self.logger.info('Connecting to Serenity database')
            conn = connect_serenity_db()
            conn.autocommit = True
            cur = conn.cursor()

            self.scheduler = HistoricNetworkScheduler()
            instrument_cache = InstrumentCache(cur, TypeCodeCache(cur))
            instruments_to_cache = [instrument_cache.get_exchange_instrument('Phemex', 'BTCUSD')]
            md_service = HistoricMarketdataService(self.scheduler, instruments_to_cache,
                                                   self.bt_env.getenv('AZURE_CONNECT_STR'),
                                                   start_time_millis, end_time_millis)
            op_service = OrderPlacerService()
            op_service.register_order_placer(f'{exchange_id}:{instance_id}',
                                             AutoFillOrderPlacer(self.scheduler, md_service))

            ctx = StrategyContext(self.scheduler, instrument_cache, md_service, op_service, self.bt_env.values)
            strategy_instance.init(ctx)
            strategy_instance.start()

    def run(self, start_time_millis: int, end_time_millis: int):
        self.scheduler.run(start_time_millis, end_time_millis)


def main(config_path: str, module: str, strategy_class: str, start_time: str = None, end_time: str = None,
         strategy_dir: str = '.'):
    init_logging()

    timestamp_fmt = '%Y-%m-%dT%H:%M:%S'
    start_time_millis = int(time.mktime(datetime.strptime(start_time, timestamp_fmt).timetuple()) * 1000)
    end_time_millis = int(time.mktime(datetime.strptime(end_time, timestamp_fmt).timetuple()) * 1000)

    engine = AlgoBacktester(config_path, module, strategy_class, strategy_dir, start_time_millis, end_time_millis)
    engine.run(start_time_millis, end_time_millis)


if __name__ == '__main__':
    fire.Fire(main)
