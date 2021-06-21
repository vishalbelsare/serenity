import asyncio
import socket
import threading

from abc import abstractmethod, ABC
from contextlib import closing

# noinspection PyPackageRequirements
import consul
import zmq

# noinspection PyPackageRequirements
from consul import Check
from flask import Flask

# noinspection PyProtectedMember
from prometheus_client import make_wsgi_app
from werkzeug.middleware.dispatcher import DispatcherMiddleware

from serenity.app.base import Application


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
        self.event_loop = asyncio.get_event_loop()
        self.get_event_loop().set_exception_handler(AIODaemon._custom_asyncio_error_handler)
        self.consul = consul.Consul()

    def get_service_id(self):
        return self.get_service_name()

    @abstractmethod
    def get_service_name(self):
        pass

    def get_event_loop(self):
        return self.event_loop

    def run_forever(self):
        self._start_http_server()
        self.get_event_loop().run_forever()

    def _start_http_server(self):
        app = Flask(self.get_service_id())

        # Add prometheus wsgi middleware to route /metrics requests
        app.wsgi_app = DispatcherMiddleware(app.wsgi_app, {
            '/metrics': make_wsgi_app(),
            '/health': AIODaemon._create_health_wsgi_app(self.get_service_id())
        })

        port = AIODaemon._find_free_port()
        threading.Thread(target=app.run, kwargs={'port': port, 'debug': False}).start()
        self.logger.info('started up HTTP server:')
        self.logger.info(f'\tOpenTelemetry: http://localhost:{port}/metrics')
        self.logger.info(f'\tHealth check: http://localhost:{port}/health')

        # register the service with Consul
        self._register_service('http', port)

        # register the health check with Consul
        http_check = Check.http(url=f'http://localhost:{port}/health', interval='1s')
        self.consul.agent.check.register(name=f'{self._get_fully_qualified_service_name("http")}/health_check',
                                         check=http_check, service_id=self._get_fully_qualified_service_id('http'))

    def _register_service(self, service_name: str, port: int):
        self.consul.agent.service.register(name=self._get_fully_qualified_service_name(service_name),
                                           service_id=self._get_fully_qualified_service_id(service_name),
                                           port=port)

    def _get_fully_qualified_service_name(self, service_name: str):
        return f'{self.get_service_name()}-{service_name}'

    def _get_fully_qualified_service_id(self, service_name: str):
        return f'{self.get_service_id()}:{service_name}'

    @staticmethod
    def _find_free_port():
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.bind(('', 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return s.getsockname()[1]

    @staticmethod
    def _custom_asyncio_error_handler(loop, context):
        # first, handle with default handler
        loop.default_exception_handler(context)

        # force shutdown
        loop.stop()

    @staticmethod
    def _create_health_wsgi_app(service_id: str):
        def health_app(environ, start_response):
            status = '200 OK'
            header = ('', '')
            if environ['PATH_INFO'] == '/favicon.ico':
                output = b''
            else:
                output = f'{service_id} is OK'.encode('utf-8')

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
        self.ctx = zmq.Context()

    def _bind_socket(self, socket_type: int, socket_name: str):
        sock = self.ctx.socket(socket_type)

        # noinspection PyUnresolvedReferences
        port = sock.bind_to_random_port('tcp://*', max_tries=100)
        self._register_service(socket_name, port)

        return sock

