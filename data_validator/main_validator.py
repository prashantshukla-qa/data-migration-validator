from src.read_yaml import get_yaml
from src.constants import Constants


def main():
    test_types = get_yaml(Constants.DATA_TEST_CONFIG_FILENAME)["test-type"]
    for test_type in test_types:
        perform_validation(test_type)


def perform_validation(test_type):
    if "row" in test_type.lower() and "count" in test_type.lower():
        print("Testing ROw Count")
    elif "schema" in test_type.lower():
        print("Validating schema of source and target data")
    elif "duplicate" in test_type.lower():
        print("Checking for duplicates")
    else:
        raise Exception(
            "%s is an invalid validation check for data migration."%test_type)
    pass

def validate_row_count():
    pass

def validate_schema():
    pass

def validate_duplicates():
    pass

if __name__ == "__main__":
    main()
