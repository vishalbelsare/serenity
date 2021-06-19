import asyncio

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
    def __init__(self):
        super().__init__()
        self.event_loop = asyncio.get_event_loop()
        self.get_event_loop().set_exception_handler(AIODaemon._custom_asyncio_error_handler)

    def get_event_loop(self):
        return self.event_loop

    def run_forever(self):
        self.get_event_loop().run_forever()

    @staticmethod
    def _custom_asyncio_error_handler(loop, context):
        # first, handle with default handler
        loop.default_exception_handler(context)

        # force shutdown
        loop.stop()


class ZeroMQDaemon(AIODaemon):
    """
    Specialized base class for long-running microservices
    that communicate with each other by passing Cap'n Proto
    messages over 0MQ-based smart sockets. It creates the
    context needed for 0MQ connectivity but does not
    actually open a socket; sub-classes can choose the
    specific socket type and messaging pattern they need.
    """
    pass
