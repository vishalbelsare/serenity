import logging
import os
from abc import ABC

import toml


class Application(ABC):
    """
    The lowest-level base class for an application in Serenity.
    Sub-classes for the particular process type, daemon or job,
    should be used rather than sub-classing this directly.
    """

    def __init__(self, config_path: str = None):
        self.defaults = Application.load_defaults()
        self.config = Application.load_config(config_path)
        self.init_logging_config()

        self.logger = logging.getLogger(__name__)

    @staticmethod
    def load_defaults():
        config_path = os.path.join(os.path.dirname(__file__), 'defaults.cfg')
        return toml.load(config_path)

    @staticmethod
    def load_config(config_path: str):
        return toml.load(config_path)

    @staticmethod
    def init_logging():
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.WARNING)
        console_logger = logging.StreamHandler()
        if os.getenv('DEBUG'):
            console_logger.setLevel(logging.DEBUG)
        else:
            console_logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s [%(threadName)s] - %(name)s - %(levelname)s - %(message)s')
        console_logger.setFormatter(formatter)
        logger.addHandler(console_logger)

    def init_logging_config(self):
        if self.config['logging']['debug']:
            os.putenv('DEBUG', '1')
        Application.init_logging()
