import asyncio
import base64
import hashlib
import hmac
import json
import logging
import time

from math import trunc

import websockets
from phemex import PhemexConnection, AuthCredentials, AuthenticationError, CredentialError, PhemexError
from tau.core import Signal, NetworkScheduler, MutableSignal, Event
from tau.signal import Map, Filter

from serenity.trading import OrderPlacer, Order, ExecutionReport, ExecType, OrderStatus, OrderFactory, MarketOrder, \
    LimitOrder, TimeInForce, ExecInst


class WebsocketAuthenticator:
    """
    Authentication mechanism for Phemex private WS API.
    """

    def __init__(self, credentials: AuthCredentials):
        self.api_key = credentials.api_key
        self.secret_key = credentials.secret_key

    def get_user_auth_message(self) -> str:
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
            "id": 1
        }
        return json.dumps(user_auth)


class AccountOrderPositionSubscriber:
    logger = logging.getLogger(__name__)

    def __init__(self, auth: WebsocketAuthenticator, scheduler: NetworkScheduler, instance_id: str = 'prod'):
        self.auth = auth
        self.scheduler = scheduler

        self.order_events = MutableSignal()
        self.scheduler.network.attach(self.order_events)

        if instance_id == 'prod':
            self.ws_uri = 'wss://phemex.com/ws'
            self.phemex = PhemexConnection()
        elif instance_id == 'test':
            self.ws_uri = 'wss://testnet.phemex.com/ws'
            self.phemex = PhemexConnection(api_url='https://testnet-api.phemex.com')
        else:
            raise ValueError(f'Unknown instance_id: {instance_id}')

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
            def __init__(self, sub: AccountOrderPositionSubscriber, json_messages: Signal):
                self.sub = sub
                self.json_messages = json_messages

            def on_activate(self) -> bool:
                if self.json_messages.is_valid():
                    msg = self.json_messages.get_value()
                    orders = msg['orders']
                    for order in orders:
                        order_id = order['orderID']
                        cl_ord_id = order['clOrdID']
                        exec_id = order['execID']
                        cum_qty = order['cumQty']
                        leaves_qty = order['leavesQty']
                        last_px = order['execPriceEp'] / 10000
                        last_qty = order['execQty']

                        exec_type = None
                        if order['action'] == 'New':
                            exec_type = ExecType.NEW
                        elif order['action'] == 'Cancel':
                            exec_type = ExecType.CANCELED

                        ord_status = None
                        if order['ordStatus'] == 'New':
                            ord_status = OrderStatus.NEW
                        elif order['ordStatus'] == 'Canceled':
                            ord_status = OrderStatus.CANCELED
                        elif order['ordStatus'] == 'PartiallyFilled':
                            ord_status = OrderStatus.PARTIALLY_FILLED
                        elif order['ordStatus'] == 'Filled':
                            ord_status = OrderStatus.FILLED

                        order_event = ExecutionReport(order_id, cl_ord_id, exec_id, exec_type,
                                                      ord_status, cum_qty, leaves_qty, last_px, last_qty)
                        self.sub.scheduler.schedule_update(self.sub.order_events, order_event)
                    return True
                else:
                    return False

        network.connect(json_messages, OrderEventScheduler(self, json_messages))

        # noinspection PyShadowingNames
        async def do_subscribe():
            async with websockets.connect(self.ws_uri) as sock:
                self.logger.info(f'sending Account-Order-Position subscription request')
                auth_msg = self.auth.get_user_auth_message()
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
                    self.scheduler.schedule_update(messages, await sock.recv())

        asyncio.ensure_future(do_subscribe())


class PhemexOrderPlacer(OrderPlacer):
    logger = logging.getLogger(__name__)

    def __init__(self, credentials: AuthCredentials, scheduler: NetworkScheduler, instance_id: str = 'prod'):
        super().__init__(OrderFactory())
        auth = WebsocketAuthenticator(credentials)
        self.aop_subscriber = AccountOrderPositionSubscriber(auth, scheduler, instance_id)
        self.aop_subscriber.start()

        if instance_id == 'prod':
            self.trading_conn = PhemexConnection(credentials)
        elif instance_id == 'test':
            self.trading_conn = PhemexConnection(credentials, api_url='https://testnet-api.phemex.com')
        else:
            raise ValueError(f'Unknown instance_id value: {instance_id}')

    def get_order_events(self) -> Signal:
        return self.aop_subscriber.get_order_events()

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

    def cancel(self, order: Order):
        symbol = order.get_instrument().get_exchange_instrument_code()
        cl_ord_id = order.get_cl_ord_id()
        order_id = order.get_order_id()
        response = self.trading_conn.send_message('DELETE', '/orders', {
            'symbol': symbol,
            'orderID': order_id
        })
        if response['data'][0]['bizError'] == 10002:
            self.logger.warn(f'too late to cancel: clOrdID={cl_ord_id}')

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
