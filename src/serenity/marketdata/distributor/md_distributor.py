import asyncio

import fire
import zmq
from prometheus_client import Counter

from serenity.app.daemon import ZeroMQPublisher
from serenity.marketdata.fh.feedhandler import feedhandler_capnp


class MarketdataDistributor(ZeroMQPublisher):
    def __init__(self, config_path: str):
        super().__init__(config_path)
        self.trade_counter = Counter('trade_republish_counter',
                                     'Number of trade prints distributed')
        self.book_delta_counter = Counter('book_delta_republish_counter',
                                          'Number of order book deltas distributed')

    def get_service_id(self):
        return 'serenity/marketdata/distributor'

    def get_service_name(self):
        return 'marketdata-distributor'

    def start_services(self):
        # open a socket for downstream consumers
        service_id = self._get_fully_qualified_service_id('publisher')
        meta = {'protocol': 'ZMQ'}
        self.pub_socket = self._bind_socket(zmq.PUB, 'marketdata-distributor-publisher', service_id, meta=meta)

        # discover all feedhandlers in the registry and subscribe to their feeds
        fh_sockets = self._connect_sockets(zmq.SUB, 'feedhandler-publisher')
        for fh_socket in fh_sockets:
            fh_socket.setsockopt(zmq.SUBSCRIBE, b'trades')
            fh_socket.setsockopt(zmq.SUBSCRIBE, b'book_deltas')
            asyncio.ensure_future(self._distribute_messages(fh_socket))

    async def _distribute_messages(self, sock):
        while True:
            msg_received = await sock.recv_multipart()
            topic = msg_received[0]
            segments = msg_received[1:]
            if topic == b'trades':
                trade = feedhandler_capnp.TradeMessage.from_segments(segments)
                topic = f'trades/{trade.symbol.feedCode}/{trade.symbol.symbolCode}'
                await self._publish_msg(topic, segments)
                self.trade_counter.inc()
            elif topic == b'book_deltas':
                delta = feedhandler_capnp.OrderBookDeltaMessage.from_segments(segments)
                topic = f'book_deltas/{delta.symbol.feedCode}/{delta.symbol.symbolCode}'
                await self._publish_msg(topic, segments)
                self.book_delta_counter.inc()
            else:
                raise ValueError(f'unknown topic: {topic}')


def main(config_path: str):
    fh = MarketdataDistributor(config_path)
    fh.run_forever()


if __name__ == '__main__':
    fire.Fire(main)
