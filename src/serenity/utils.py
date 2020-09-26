import logging
import os
from enum import Enum


def init_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    console_logger = logging.StreamHandler()
    console_logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_logger.setFormatter(formatter)
    logger.addHandler(console_logger)


def custom_asyncio_error_handler(loop, context):
    # first, handle with default handler
    loop.default_exception_handler(context)

    # force shutdown
    loop.stop()


class Environment:
    def __init__(self, config_yaml, parent=None):
        self.values = parent.values if parent is not None else {}
        for entry in config_yaml:
            key = entry['key']
            value = None
            if 'value' in entry:
                value = entry['value']
            elif 'value-source' in entry:
                source = entry['value-source']
                if source == 'SYSTEM_ENV':
                    value = os.getenv(key)
            else:
                raise ValueError(f'Unsupported value type in Environment entry: {entry}')

            self.values[key] = value

    def getenv(self, key: str, default_val: str = None) -> str:
        if key in self.values:
            value = self.values[key]
            if value is None or value == '':
                return default_val
            else:
                return value
        else:
            return default_val
