from utils.read_yaml import get_yaml, read_yaml
from utils.file_utils import list_test_yamls
from utils.constants import Constants
from data_migrator import mysql2mongodb
from data_validators import validate_data   
import argparse

test_options = get_yaml(Constants.DATA_TEST_CONFIG_FILENAME)
migration_options = get_yaml(Constants.DATABASE_CONFIG_FILENAME)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", nargs='?', const="Validation", default="Validation", help="Action to be performed"
                        + " Options are: 1. Validation 2. Migration")
    args = parser.parse_args()
    if args.action.lower() == "validation":
        execute_validation_tests()
    elif args.action.lower() == "migration":
        perform_migration()
    else:
        raise Exception(
            "Valid action arguments are 1. Validation 2. Migration")


def perform_migration():
    print("="*20)
    print("Migrating DataBase from %s to %s" % (
        migration_options["migration"]["source"], migration_options["migration"]["target"]))
    print("="*20 + "\n")
    mysql2mongodb.main()


def execute_validation_tests():
    test_files = list_test_yamls(Constants.TEST_FILE_LOC)
    for file in test_files:
        test_case = read_yaml(file)
        print("\nExecuting test:- " + test_case["test_name"])
        print ("There are " + str(len(test_case["data-validations"]["steps"])) + " validations in the test case")
        perform_validation(test_case)


def perform_validation(test_case):
    if not test_case["data-validations"]["to-be-performed"]:
        print("Skipping the test '" + test_case["test_name"] +
              "' as it is marked as false in to-be-performed test option.\n")
    else:
        for test_type in test_case["data-validations"]["steps"]:
            if "row" in test_type.lower() and "count" in test_type.lower():
                print("\nValidating Row Count Post Migration")
                print("="*20)
                validate_data.check_table_row_count()
            elif "schema" in test_type.lower():
                print("\nValidating schema of source and target data")
                print("="*20)
                validate_data.check_for_table_schema(test_case)
            elif "duplicate" in test_type.lower():
                print("\nChecking for duplicates")
                print("="*20)
                validate_data.check_for_duplicates()
            elif "validate_min_max" in test_type.lower():
                print("\nChecking for Min Max Values")
                print("="*20)
            else:
                raise Exception(
                    "%s is an invalid validation check for data migration." % test_type)
        print("Test case '" + test_case["test_name"] + "' Execution Completed\n")


if __name__ == "__main__":
    main()
