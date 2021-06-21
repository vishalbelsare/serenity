import os
from shutil import copyfile

from serenity.utils.config import TOMLProcessor


def test_toml_load_plain():
    cfg = TOMLProcessor.load(get_test_fixture('simple.cfg'))
    assert len(cfg) == 1
    assert cfg['baz']['foo'] == 'bar'


def test_toml_load_include_absolute():
    fixture_path = get_test_fixture('simple.cfg')
    copyfile(fixture_path, '/tmp/simple.cfg')
    cfg = TOMLProcessor.load(get_test_fixture('absolute_include.cfg'))
    assert len(cfg) == 1
    assert cfg['baz']['foo'] == 'bar+'


def test_toml_load_include_relative():
    cfg = TOMLProcessor.load(get_test_fixture('relative_include.cfg'))
    assert len(cfg) == 3
    assert len(cfg['azure']) == 3
    assert cfg['baz']['foo'] == 'bar*'
    assert cfg['baz']['bing'] == 'bong'
    assert cfg['boo']['bash'] == 'foobar'


def get_test_fixture(rel_path: str):
    return os.path.join(os.path.dirname(__file__), rel_path)
