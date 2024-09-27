import pytest
from ..src.read_yaml import get_yaml

class TestFunctions:

    def test_yaml_reader(self):
        yamlout = get_yaml("config")
        print (yamlout)
        assert 1==1
        