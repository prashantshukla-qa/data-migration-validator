import yaml
from files import list_yamls

def read_yaml(path):
    with open(path, 'r') as f:
        d = yaml.safe_load(f)
        return d

yaml_files = list_yamls('.')

yaml_data = read_yaml(yaml_files[0])

print (yaml_data['source_db'])
