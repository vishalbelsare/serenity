import asyncio
import socket
import threading
import uvloop

from abc import abstractmethod, ABC
from collections import defaultdict
from contextlib import closing

# noinspection PyPackageRequirements
import consul
import zmq

from zmq.asyncio import Context, Socket

# noinspection PyPackageRequirements
from consul import Check
from flask import Flask

# noinspection PyProtectedMember
from prometheus_client import make_wsgi_app, Summary, Counter
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from zmq.utils.monitor import parse_monitor_message

from serenity.app.base import Application
from serenity.utils.metrics import HighPerformanceTimer

# build up an event map for use in monitoring
EVENT_MAP = {}
for name in dir(zmq):
    if name.startswith('EVENT_'):
        value = getattr(zmq, name)
        EVENT_MAP[value] = name


class AIODaemon(Application):
    """
    Base class for long-running microservices that use
    Python's asyncio as a master event loop. The base
    starts up, runs an event loop and starts a Flask-based
    HTTP server on a random port. The HTTP server exports
    an OpenTelemetry metrics endpoint at /metrics, which it
    registers with the Consul agent. It also exports a basic
    health check REST endpoint for Consul's use at /health.
    """

    def __init__(self, config_path: str):
        super().__init__(config_path)
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        self.event_loop = asyncio.get_event_loop()

        consul_agent_host = self.get_config('consul', 'consul_agent_host', 'localhost')
        consul_agent_port = self.get_config('consul', 'consul_agent_port', '8500')
        self.logger.info(f'connecting to Consul Agent on {consul_agent_host}:{consul_agent_port}')
        self.consul = consul.Consul(host=consul_agent_host,
                                    port=consul_agent_port)

        self.http_host = None
        self.http_port = None

    def get_service_id(self):
        return self.get_service_name()

    @abstractmethod
    def get_service_name(self):
        pass

    def start_services(self):
        pass

    def get_event_loop(self):
        return self.event_loop

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def is_service_healthy(self, service_id: str):
        return True

    def run_forever(self):
        self._start_http_server()
        self.get_event_loop().call_soon(self.start_services)
        self.get_event_loop().run_forever()

    # noinspection HttpUrlsUsage
    def _start_http_server(self):
        app = Flask(self.get_service_id())
        http_service_id = self.get_service_id()

        # Add prometheus wsgi middleware to route /metrics requests
        app.wsgi_app = DispatcherMiddleware(app.wsgi_app, {
            '/metrics': make_wsgi_app(),
            '/health': self._create_health_wsgi_app(http_service_id)
        })

        self.http_host = self.get_config('networking', 'hostname')
        self.http_port = AIODaemon._find_free_port()
        threading.Thread(target=app.run, kwargs={'host': self.http_host, 'port': self.http_port,
                                                 'debug': False}).start()
        self.logger.info('started up HTTP server:')
        self.logger.info(f'\tOpenTelemetry: http://{self.http_host}:{self.http_port}/metrics')
        self.logger.info(f'\tHealth check: http://{self.http_host}:{self.http_port}/health')

        # register the service with Consul
        self._register_service(self.get_service_name(), self.get_service_id(), self.http_port)

        # register the health check with Consul
        self._register_health_check(http_service_id)

    def _register_service(self, service_name: str, service_id: str, port: int, tags: list = None, meta: dict = None):
        host = self.get_config('networking', 'hostname')
        self.logger.debug(f'registering service={service_name}, service_id={service_id} on {host}:{port}')
        self.consul.agent.service.register(name=service_name, service_id=service_id, address=host, port=port,
                                           tags=tags, meta=meta)

    # noinspection HttpUrlsUsage
    def _register_health_check(self, service_id: str, interval: str = '1s'):
        health_check_url = f'http://{self.http_host}:{self.http_port}/health/{service_id}'
        http_check = Check.http(url=health_check_url, interval=interval)
        self.consul.agent.check.register(name=f'{service_id}/health-check',
                                         check=http_check, service_id=service_id)
        self.logger.debug(f'registered health check endpoint for {service_id}: {health_check_url}')

    def _get_fully_qualified_service_name(self, service_name: str):
        return f'{self.get_service_name()}-{service_name}'

    def _get_fully_qualified_service_id(self, service_name: str):
        return f'{self.get_service_id()}-{service_name}'

    @staticmethod
    def _find_free_port():
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.bind(('', 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return s.getsockname()[1]

    def _create_health_wsgi_app(self, http_service_id):
        def health_app(environ, start_response):
            status = '200 OK'
            header = ('', '')
            if environ['PATH_INFO'] == '/favicon.ico':
                output = b''
            else:
                service_id = environ['PATH_INFO'][1:]
                if service_id == http_service_id:
                    healthy = True
                else:
                    healthy = self.is_service_healthy(service_id)
                if healthy:
                    output = f'{service_id} is OK'.encode('utf-8')
                else:
                    output = f'{service_id} is not OK'.encode('utf-8')
                    status = '500 Failed'

            # Return output
            start_response(status, [header])
            return [output]

        return health_app


class ZeroMQDaemon(AIODaemon, ABC):
    """
    Specialized base class for long-running microservices
    that communicate with each other by passing Cap'n Proto
    messages over 0MQ-based smart sockets. It creates the
    context needed for 0MQ connectivity but does not
    actually open a socket; sub-classes can choose the
    specific socket type and messaging pattern they need.
    """

    def __init__(self, config_path: str):
        super().__init__(config_path)
        self.ctx = Context.instance()
        self.service_socket_up = defaultdict(bool)
        self.connect_counters = KeyDefaultDict(lambda key: Counter(f'{key}_connects',
                                                                   f'{key} - ZeroMQ socket connections accepted'))
        self.disconnect_counters = KeyDefaultDict(lambda key: Counter(f'{key}_disconnects',
                                                                      f'{key} - ZeroMQ socket disconnects'))

    def is_service_healthy(self, service_id: str):
        return self.service_socket_up[service_id]

    def _connect_sockets(self, socket_type: int, service_name: str, tag: str = None):
        (index, services) = self.consul.catalog.service(service_name, tag=tag)
        sockets = []
        for service in services:
            sock = self.ctx.socket(socket_type)
            host = service['Address']
            port = service['ServicePort']
            tags = service['ServiceTags']
            self.logger.debug(f'discovered {service_name} on {host}:{port}; tags={tags}')
            sock.connect(f'tcp://{host}:{port}')
            sockets.append(sock)
        return sockets

    # noinspection PyUnresolvedReferences
    def _bind_socket(self, socket_type: int, service_name: str,
                     service_id: str, tags: list = None,
                     meta: dict = None):
        sock = self.ctx.socket(socket_type)
        monitor = sock.get_monitor_socket()
        asyncio.ensure_future(self._event_monitor(monitor, service_name, service_id))

        # noinspection PyUnresolvedReferences
        port = sock.bind_to_random_port('tcp://*', max_tries=100)
        self._register_service(service_name, service_id, port, tags, meta)
        self._register_health_check(service_id)

        return sock

    async def _event_monitor(self, monitor: Socket, service_name: str, service_id: str):
        self.logger.debug(f'monitoring for {service_name} ZeroMQ socket starting')
        metric_key = service_name.replace('-', '_')

        # initialize connection metrics
        self.connect_counters[metric_key].inc(0)
        self.disconnect_counters[metric_key].inc(0)

        while await monitor.poll():
            msg = await monitor.recv_multipart()
            evt = parse_monitor_message(msg)
            event_id = evt['event']
            if event_id == zmq.EVENT_MONITOR_STOPPED:
                break
            elif event_id == zmq.EVENT_LISTENING:
                self.service_socket_up[service_id] = True
                self.logger.debug(f'{service_name} ZeroMQ socket is UP')
            elif event_id == zmq.EVENT_ACCEPTED:
                self.connect_counters[metric_key].inc()
            elif event_id == zmq.EVENT_DISCONNECTED:
                self.disconnect_counters[metric_key].inc()
            else:
                event_name = EVENT_MAP[event_id]
                self.logger.debug(f'received ZeroMQ monitoring event: {event_name}')

        monitor.close()
        self.logger.warn(f'monitoring for {service_name} ZeroMQ socket terminated')
        self.service_socket_up[service_id] = False

    def _is_socket_service_up(self, service_name: str):
        return self.service_socket_up[service_name]


class ZeroMQPublisher(ZeroMQDaemon, ABC):
    def __init__(self, config_path: str):
        super().__init__(config_path)
        self.pub_socket = None
        self.msg_pub_latency = Summary('msg_pub_latency_us', 'ZeroMQ message publication latency (us)')

    async def _publish_msg(self, topic: str, msg_segments: list):
        with HighPerformanceTimer(self.msg_pub_latency.observe):
            await self.pub_socket.send(topic.encode('utf8'), zmq.SNDMORE)
            for count, msg_segment in enumerate(msg_segments):
                if count == len(msg_segments) - 1:
                    await self.pub_socket.send(msg_segment)
                else:
                    await self.pub_socket.send(msg_segment, zmq.SNDMORE)


class KeyDefaultDict(defaultdict):
    def __init__(self, keyed_default_factory):
        super().__init__(None)
        self.keyed_default_factory = keyed_default_factory

    def __missing__(self, key):
        ret = self.keyed_default_factory(key)
        self[key] = ret
        return ret
