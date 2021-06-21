import mergedeep
import os
import re
import toml

defaults = dict()
decoder = toml.TomlDecoder()
RE_ENV_VAR = r'\$\{?([A-Z_][A-Z0-9_]+)\}?'


def init_defaults():
    global defaults
    config_path = os.path.join(os.path.dirname(__file__), '../defaults.cfg')
    defaults = toml.load(config_path)


def get_global_defaults() -> dict:
    if len(defaults) == 0:
        init_defaults()
    return defaults


class TOMLProcessor:
    """
    A high-level wrapper around the basic TOML interface that provides
    for additional functionality -- currently limited to supporting
    a custom "include" declaration. This is a list-typed value containing
    relative or absolute paths to other TOML files to load first. The
    entire graph of config files gets loaded recursively and then a
    single merged dictionary is returned.
    """

    @staticmethod
    def load(config_path: str):
        target = {}
        TOMLProcessor._merge_load(config_path, target)
        TOMLProcessor._postprocess(target)
        return target

    @staticmethod
    def _merge_load(config_path: str, target: dict):
        toml_dict = toml.load(config_path)
        if 'include' in toml_dict.keys():
            include_paths = toml_dict['include']
            for include_path in include_paths:
                if include_path.startswith('/'):
                    load_path = include_path
                else:
                    load_path = os.path.join(os.path.dirname(config_path), include_path)
                TOMLProcessor._merge_load(load_path, target)
            del toml_dict['include']
        mergedeep.merge(target, toml_dict)

    @staticmethod
    def _postprocess(item):
        iter_ = None
        if isinstance(item, dict):
            iter_ = item.items()
        elif isinstance(item, list):
            iter_ = enumerate(item)

        for i, val in iter_:
            if isinstance(val, (dict, list)):
                TOMLProcessor._postprocess(val)
            elif isinstance(val, str):
                if re.match(RE_ENV_VAR, val):
                    r = re.sub(RE_ENV_VAR, TOMLProcessor._env_replace, val)

                    # Try to first load the value from the environment variable
                    # (i.e. make what seems like a float a float, what seems like a
                    # boolean a bool and so on). If that fails, fail back to
                    # string.
                    try:
                        item[i], _ = decoder.load_value(r)
                        continue
                    except ValueError:
                        pass

                    item[i], _ = decoder.load_value('"{}"'.format(r))

    @staticmethod
    def _env_replace(x):
        env_var = x.groups()[0]
        return os.environ.get(env_var, '')


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
