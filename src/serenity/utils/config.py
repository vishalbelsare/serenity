import os

import toml

defaults = dict()


def init_defaults():
    global defaults
    config_path = os.path.join(os.path.dirname(__file__), 'defaults.cfg')
    defaults = toml.load(config_path)


def get_global_defaults() -> dict:
    if len(defaults) == 0:
        init_defaults()
    return defaults


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
