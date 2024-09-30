import yaml
import logging
from .file_utils import list_yamls, list_test_yamls


def read_yaml(path):
    with open(path, 'r') as f:
        d = yaml.safe_load(f)
        return d


def get_yaml(filename):
    filenames = list_yamls('.')
    try:
        for file in filenames:
            if filename.lower() in file.lower():
                return read_yaml(file)
    except:
        logging.exception(
            "There are no yaml file by the name %s.yaml in the root " % file)


def get_all_yamls():
    filenames = list_test_yamls
    yamls = {}
    try:
        for file in filenames:
            if filenames.lower() in file.lower():
                return read_yaml(file)
    except:
        logging.exception(
            "There are no yaml file by the name %s.yaml in the root " % file)


if __name__ == "__main__":
    pass
