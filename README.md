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

### *Note:*
- The sample data to populate mySQL db is given in the [sample data](./db_scripts/mysql/data_dump_mysql/) folder
