import logging
import os
from abc import ABC

import toml

from serenity.utils.config import TOMLProcessor


class Application(ABC):
    """
    The lowest-level base class for an application in Serenity.
    Sub-classes for the particular process type, daemon or job,
    should be used rather than sub-classing this directly.
    """

    def __init__(self, config_path: str):
        self.defaults = Application.load_defaults()
        self.config = Application.load_config(config_path)
        self.init_logging_config()

        self.logger = logging.getLogger(self._get_subclass_name())

    @staticmethod
    def load_defaults():
        config_path = os.path.join(os.path.dirname(__file__), '../defaults.cfg')
        return toml.load(config_path)

    @staticmethod
    def load_config(config_path: str):
        if config_path:
            return TOMLProcessor.load(config_path)
        else:
            return None

    @staticmethod
    def init_logging(log_level: str = None):
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)

        # suppress some particularly noisy loggers
        logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.WARNING)
        logging.getLogger('werkzeug').setLevel(logging.WARNING)
        logging.getLogger('urllib3.connectionpool').setLevel(logging.INFO)
        logging.getLogger('websockets.protocol').setLevel(logging.INFO)
        logging.getLogger('websockets.server').setLevel(logging.INFO)

        console_logger = logging.StreamHandler()
        if log_level:
            console_logger.setLevel(logging.getLevelName(log_level))
        else:
            console_logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s [%(threadName)s] - %(name)s - %(levelname)s - %(message)s')
        console_logger.setFormatter(formatter)
        logger.addHandler(console_logger)

    @staticmethod
    def _get_config(config: dict, section: str, key: str, default_val: str = None):
        if not config:
            return default_val
        else:
            if section not in config.keys():
                return default_val
            elif key not in config[section].keys():
                return default_val
            else:
                return config[section][key]

    def init_logging_config(self):
        # this is a workaround to allow us to migrate existing
        # code to the new framework: we will dispense with the
        # environment variable and static method later
        log_level = self.get_config('logging', 'log_level', 'INFO').upper()
        Application.init_logging(log_level)

    def get_default(self, section: str, key: str, default_val: str = None):
        return Application._get_config(self.defaults, section, key, default_val)

    def get_config(self, section: str, key: str, default_val: str = None):
        return Application._get_config(self.config, section, key, default_val)

    def _get_subclass_name(self):
        return self.__class__.__name__
