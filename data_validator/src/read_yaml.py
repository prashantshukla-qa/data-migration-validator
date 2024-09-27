import yaml
import logging
from . import files

def read_yaml(path):
    with open(path, 'r') as f:
        d = yaml.safe_load(f)
        return d

def get_yaml(filename):
    filenames = files.list_yamls('.')
    print (filenames)
    try:
        for file in filenames:
            if filename.lower() in file.lower():
                return read_yaml(file)
    except:
        logging.exception ("There are no yaml file by the name %s.yaml in the root " %file)


if __name__=="__main__":
    pass