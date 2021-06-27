import asyncio

import fire
import zmq
from prometheus_client import Counter
from tau.core import RealtimeNetworkScheduler

from serenity.app.daemon import ZeroMQPublisher
from serenity.marketdata.fh.feedhandler import feedhandler_capnp


class OHLCPublisher(ZeroMQPublisher):
    def __init__(self, config_path: str):
        super().__init__(config_path)

        # open a socket for downstream consumers
        service_id = self._get_fully_qualified_service_id('publisher')
        meta = {'protocol': 'ZMQ'}
        self.pub_socket = self._bind_socket(zmq.PUB, 'ohlcv-publisher', service_id, meta=meta)

        self.trade_counter = Counter('trade_counter', 'Number of trade prints received from distributor')
        self.ohlcv_1min_update_counter = Counter('ohlcv_1min_update_counter',
                                                 'Number of OHLCV 1 minute bin updates published')
        self.ohlcv_5min_update_counter = Counter('ohlcv_5min_update_counter',
                                                 'Number of OHLCV 5 minute bin updates published')

        # discover the marketdata distributors in the registry and subscribe to all trade prints;
        # this needs to be replaced with the topology server when it is ready
        fh_sockets = self._connect_sockets(zmq.SUB, 'marketdata-distributor-publisher')
        for fh_socket in fh_sockets:
            fh_socket.setsockopt(zmq.SUBSCRIBE, b'trades')
            asyncio.ensure_future(self._process_messages(fh_socket))

        # set up the graph scheduler
        self.scheduler = RealtimeNetworkScheduler()

    def get_service_id(self):
        return 'serenity/marketdata/ohclv-publisher'

    def get_service_name(self):
        return 'ohclv-publisher'

    async def _process_messages(self, sock):
        while True:
            msg_received = await sock.recv_multipart()
            topic = msg_received[0]
            segments = msg_received[1:]
            if topic[0:6] == b'trades':
                trade = feedhandler_capnp.TradeMessage.from_segments(segments)
                self._schedule_trade_processing(topic, trade)
            else:
                raise ValueError(f'unknown topic: {topic}')

    # noinspection PyUnusedLocal
    def _schedule_trade_processing(self, topic: str, trade: feedhandler_capnp.TradeMessage):
        self.trade_counter.inc()


def main(config_path: str):
    fh = OHLCPublisher(config_path)
    fh.run_forever()


if __name__ == '__main__':
    fire.Fire(main)
