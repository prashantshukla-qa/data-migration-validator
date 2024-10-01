# data-migration-validator
Migrate data from MySQL to MongoDB and validate the sanctity of the data post migration. This project uses **MySQL to MongoDB** migration for demostrating the validation checks post data migration.

### Requirements and Installation

- Require Python 3+
- Install dependecies

        $> pip install -r requirement.txt

### Usage

1. Update the Connection strings in the [database_config.yaml](./database_config.yaml) file.
1. Put the test cases in the [test_suite](./test_suite/) folder
1. The test cases are the yaml files providing list of validations and source and target database

- Run following command from the command line

        $> python main_validator.py

### Main Validator Options

- Run Checks / Validations 
    
        $> python main_validator.py

- Perform Data Migration

        %> pthon main_validator.py Migration


### Test Case Template

Following test case template can be used to create new tests. Keep the test_case.yaml file in the [test_suite](./test_suite/) folder. All the test in test suite folder will be executed provided **to-be-performed** value is true.
        
        test_name: "Test Case 1"
        data-validations:
        source: mysql
        target: mongodb
        to-be-performed: true
        steps:
                - validate_row_table_count
                - validate_schema
                - validate_min_max

#### Test Keyords available

As of now following test keywords can be used for creating the test cases:
- **validate_row_table_count**
    - This get all the tables and respective row count from source and matches with the target db
- **check_for_duplicates**
    - This checks for any duplicate data create in the target database after migration
- **validate_min_max**
    - This validates that the min and max values for each column in each table from the source is retained in the target database
- **validate_schema**
    - Use this to validate that the schema from source is maintained in the target database


### *Note:*
- The sample data to populate mySQL db is given in the [sample data](./db_scripts/mysql/data_dump_mysql/) folder
