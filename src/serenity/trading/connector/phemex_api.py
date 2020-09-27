import asyncio
import base64
import hashlib
import hmac
import json
import logging
import time

from math import trunc
from typing import Any

import websockets
from phemex import PhemexConnection, AuthCredentials, AuthenticationError, CredentialError, PhemexError
from tau.core import Signal, NetworkScheduler, MutableSignal, Event
from tau.signal import Map, Filter

from serenity.db import InstrumentCache
from serenity.position import ExchangePositionService, ExchangePosition
from serenity.trading import OrderPlacer, Order, OrderFactory, MarketOrder, LimitOrder, TimeInForce, ExecInst, \
    StopOrder, OrderManagerService


def get_phemex_connection(credentials: AuthCredentials, instance_id: str = 'prod') -> Any:
    if instance_id == 'prod':
        ws_uri = 'wss://phemex.com/ws'
        return PhemexConnection(credentials), ws_uri
    elif instance_id == 'test':
        ws_uri = 'wss://testnet.phemex.com/ws'
        return PhemexConnection(credentials, api_url='https://testnet-api.phemex.com'), ws_uri
    else:
        raise ValueError(f'Unknown instance_id: {instance_id}')


class WebsocketAuthenticator:
    """
    Authentication mechanism for Phemex private WS API.
    """

    def __init__(self, credentials: AuthCredentials):
        self.api_key = credentials.api_key
        self.secret_key = credentials.secret_key

    def get_user_auth_message(self, auth_id: int = 1) -> str:
        expiry = trunc(time.time()) + 60
        token = self.api_key + str(expiry)
        token = token.encode('utf-8')
        hmac_key = base64.urlsafe_b64decode(self.secret_key)
        signature = hmac.new(hmac_key, token, hashlib.sha256)
        signature_b64 = signature.hexdigest()

        user_auth = {
            "method": "user.auth",
            "params": [
                "API",
                self.api_key,
                signature_b64,
                expiry
            ],
            "id": auth_id
        }
        return json.dumps(user_auth)


# noinspection DuplicatedCode
class OrderEventSubscriber:
    logger = logging.getLogger(__name__)

    def __init__(self, credentials: AuthCredentials, scheduler: NetworkScheduler, oms: OrderManagerService,
                 instance_id: str = 'prod'):
        self.auth = WebsocketAuthenticator(credentials)
        self.scheduler = scheduler
        self.oms = oms

        self.order_events = MutableSignal()
        self.scheduler.network.attach(self.order_events)

        (self.phemex, self.ws_uri) = get_phemex_connection(credentials, instance_id)

    def get_order_events(self) -> Signal:
        return self.order_events

    def start(self):
        network = self.scheduler.get_network()
        messages = MutableSignal()
        json_messages = Map(network, messages, lambda x: json.loads(x))
        json_messages = Filter(network, json_messages,
                               lambda x: x.get('type', None) == 'incremental')

        class OrderEventScheduler(Event):
            # noinspection PyShadowingNames
            def __init__(self, sub: OrderEventSubscriber, json_messages: Signal):
                self.sub = sub
                self.json_messages = json_messages

            def on_activate(self) -> bool:
                if self.json_messages.is_valid():
                    msg = self.json_messages.get_value()
                    orders = msg['orders']
                    for order_msg in orders:
                        order_id = order_msg['orderID']
                        exec_id = order_msg['execID']
                        last_px = order_msg['execPriceEp'] / 10000
                        last_qty = order_msg['execQty']

                        order = self.sub.oms.get_order_by_order_id(order_id)
                        if order is None:
                            self.sub.logger.warning(f'Ignored unknown orderID={order_id}')
                            continue

                        if order_msg['ordStatus'] == 'New':
                            self.sub.oms.new(order, exec_id)
                        elif order_msg['ordStatus'] == 'Canceled':
                            self.sub.oms.apply_cancel(order, exec_id)
                        elif order_msg['ordStatus'] == 'PartiallyFilled' or order_msg['ordStatus'] == 'Filled':
                            self.sub.oms.apply_fill(order, last_px, last_qty, exec_id)

                    return True
                else:
                    return False

        network.connect(json_messages, OrderEventScheduler(self, json_messages))

        # noinspection PyShadowingNames,PyBroadException
        async def do_subscribe():
            async with websockets.connect(self.ws_uri) as sock:
                self.logger.info(f'sending Account-Order-Position subscription request for orders')
                auth_msg = self.auth.get_user_auth_message(1)
                await sock.send(auth_msg)
                error_msg = await sock.recv()
                error_struct = json.loads(error_msg)
                if error_struct['error'] is not None:
                    raise ConnectionError(f'Unable to authenticate: {error_msg}')

                aop_sub_msg = {
                    'id': 2,
                    'method': 'aop.subscribe',
                    'params': []
                }
                await sock.send(json.dumps(aop_sub_msg))
                while True:
                    try:
                        self.scheduler.schedule_update(messages, await sock.recv())
                    except BaseException as error:
                        self.logger.info(f'disconnected; attempting to reconnect: {error}')
                        asyncio.ensure_future(do_subscribe())
                        break

        asyncio.ensure_future(do_subscribe())


# noinspection DuplicatedCode
class PhemexExchangePositionService(ExchangePositionService):
    logger = logging.getLogger(__name__)

    def __init__(self, credentials: AuthCredentials, scheduler: NetworkScheduler, instrument_cache: InstrumentCache,
                 account: str, instance_id: str = 'prod'):
        super().__init__(scheduler)
        self.auth = WebsocketAuthenticator(credentials)
        self.scheduler = scheduler
        self.instrument_cache = instrument_cache
        self.account = account

        self.order_events = MutableSignal()
        self.scheduler.network.attach(self.order_events)

        (self.phemex, self.ws_uri) = get_phemex_connection(credentials, instance_id)

    def subscribe(self):
        network = self.scheduler.get_network()
        messages = MutableSignal()
        json_messages = Map(network, messages, lambda x: json.loads(x))

        class PositionUpdateScheduler(Event):
            # noinspection PyShadowingNames
            def __init__(self, sub: PhemexExchangePositionService, json_messages: Signal):
                self.sub = sub
                self.json_messages = json_messages

            def on_activate(self) -> bool:
                if self.json_messages.is_valid():
                    msg = self.json_messages.get_value()
                    if 'positions' in msg:
                        for position in msg['positions']:
                            if position['accountID'] == self.sub.account:
                                qty = (position['crossSharedBalanceEv'] / 100_000_000)
                                ccy_symbol = position['currency']
                                ccy = self.sub.instrument_cache.get_or_create_currency(ccy_symbol)
                                xp = ExchangePosition(self.sub.account, ccy, qty)
                                self.sub.scheduler.schedule_update(self.sub.exchange_positions, xp)

                    return True
                else:
                    return False

        network.connect(json_messages, PositionUpdateScheduler(self, json_messages))

        # noinspection PyShadowingNames
        async def do_subscribe():
            async with websockets.connect(self.ws_uri) as sock:
                self.logger.info(f'sending Account-Order-Position subscription request for positions')
                auth_msg = self.auth.get_user_auth_message(2)
                await sock.send(auth_msg)
                error_msg = await sock.recv()
                error_struct = json.loads(error_msg)
                if error_struct['error'] is not None:
                    raise ConnectionError(f'Unable to authenticate: {error_msg}')

                aop_sub_msg = {
                    'id': 3,
                    'method': 'aop.subscribe',
                    'params': []
                }
                await sock.send(json.dumps(aop_sub_msg))
                while True:
                    try:
                        self.scheduler.schedule_update(messages, await sock.recv())
                    except BaseException as error:
                        self.logger.info(f'disconnected; attempting to reconnect: {error}')
                        asyncio.ensure_future(do_subscribe())
                        break

        asyncio.ensure_future(do_subscribe())


class PhemexOrderPlacer(OrderPlacer):
    logger = logging.getLogger(__name__)

    def __init__(self, credentials: AuthCredentials, scheduler: NetworkScheduler, oms: OrderManagerService,
                 account: str, instance_id: str = 'prod'):
        super().__init__(OrderFactory(account))
        self.oms = oms

        self.oe_subscriber = OrderEventSubscriber(credentials, scheduler, oms, instance_id)
        self.oe_subscriber.start()

        (self.trading_conn, ws_uri) = get_phemex_connection(credentials, instance_id)

    def submit(self, order: Order):
        params = dict()
        params['actionBy'] = 'FromOrderPlacement'
        params['clOrdID'] = order.get_cl_ord_id()
        params['symbol'] = order.get_instrument().get_exchange_instrument_code()
        params['orderQty'] = order.get_qty()
        params['side'] = order.get_side().name.lower().capitalize()
        if isinstance(order, LimitOrder):
            params['ordType'] = 'Limit'
            params['priceEp'] = PhemexOrderPlacer.__get_scaled_price(order.get_price())
            params['timeInForce'] = PhemexOrderPlacer.__get_tif_code(order.get_time_in_force())
            params['reduceOnly'] = order.get_exec_inst() == ExecInst.PARTICIPATE_DONT_INITIATE
        elif isinstance(order, MarketOrder):
            params['ordType'] = 'Market'
        elif isinstance(order, StopOrder):
            params['ordType'] = 'Stop'
            params['stopPxEp'] = PhemexOrderPlacer.__get_scaled_price(order.get_stop_px())
            params['triggerType'] = 'ByLastPrice'

        else:
            raise ValueError(f'unsupported Order type: {type(order)}')

        response = self.trading_conn.send_message('POST', '/orders', data=json.dumps(params))
        error_code = int(response.get('code', 200))
        if error_code > 200:
            if error_code == 10500:
                raise AuthenticationError()
            elif error_code == 401:
                raise CredentialError()
            else:
                raise PhemexError(error_code)

        order_id = response['data']['orderID']
        order.set_order_id(order_id)
        self.oms.pending_new(order)

    def cancel(self, order: Order):
        symbol = order.get_instrument().get_exchange_instrument_code()
        cl_ord_id = order.get_cl_ord_id()
        order_id = order.get_order_id()
        response = self.trading_conn.send_message('DELETE', '/orders', {
            'symbol': symbol,
            'orderID': order_id
        })
        if response['data'][0]['bizError'] == 10002:
            self.logger.warning(f'too late to cancel: clOrdID={cl_ord_id}')
        self.oms.pending_cancel(order)

    @classmethod
    def __get_scaled_price(cls, price: float) -> int:
        return int(price * 10000)

    @classmethod
    def __get_tif_code(cls, tif: TimeInForce) -> str:
        if tif == TimeInForce.DAY:
            return 'Day'
        elif tif == TimeInForce.GTC:
            return 'GoodTillCancel'
        elif tif == TimeInForce.IOC:
            return 'ImmediateOrCancel'
        elif tif == TimeInForce.FOK:
            return 'FillOrKill'
        else:
            raise ValueError(f'unsupported TimeInForce: {tif.name}')
