from src.read_yaml import get_yaml
from src.constants import Constants
from src.data_migrator import mysql2mongodb

test_options = get_yaml(Constants.DATA_TEST_CONFIG_FILENAME)


def main():
    if test_options["data-validations"]["to-be-performed"]:
        for test_type in test_options["data-validations"]["type"]:
            perform_validation(test_type)
    if test_options["data-migration"]["to-be-performed"]:
        perform_migration()


def perform_migration():
    print("="*20)
    print("Migrating DataBase from %s to %s" % (
        test_options["data-migration"]["source"], test_options["data-migration"]["target"]))
    print("="*20 + "\n")
    mysql2mongodb.main()


def perform_validation(test_type):
    if "row" in test_type.lower() and "count" in test_type.lower():
        print("Validating Row Count Post Migration")
        print("="*20)
    elif "schema" in test_type.lower():
        print("Validating schema of source and target data")
        print("="*20)
    elif "duplicate" in test_type.lower():
        print("Checking for duplicates")
        print("="*20)
    else:
        raise Exception(
            "%s is an invalid validation check for data migration." % test_type)
    pass


def validate_row_count():
    pass


def validate_schema():
    pass


def validate_duplicates():
    pass


if __name__ == "__main__":
    main()
