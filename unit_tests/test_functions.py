from utils.read_yaml import get_yaml
from database_connectors import mysql_conn, mongodb_conn
from utils.constants import Constants
from utils.file_utils import list_test_yamls
from data_validators import validate_data
import utils.file_utils
import pathlib


class TestFunctions:

    configfilename = Constants.DATABASE_CONFIG_FILENAME

    def test_yaml_reader(self):
        yamlout = get_yaml(self.configfilename)["mysql"]
        print(yamlout["database"])
        print(type(yamlout["database"]))
        assert yamlout

    def test_mysql_db_connections(self):
        connection_yaml = get_yaml(self.configfilename)["mysql"]
        db_row_count = mysql_conn.get_mysql_table_row_count(
            connection_yaml=connection_yaml)
        print(db_row_count)
        assert len(db_row_count) == 8

    def test_mongodb_document_read(self):
        document_list = mongodb_conn.get_mongodb_documents(
            get_yaml(self.configfilename)["mongodb"], "offices")
        print(len(document_list))
        print(type(document_list))
        print(document_list[0]["officeCode"])

    def test_table_row_count(self):
        validate_data.check_table_row_count()
        
    def test_duplicates(self):
        validate_data.check_for_duplicates()
        
    def test_get_primary_keys(self):
        print (mysql_conn.get_mysql_primary_keys(get_yaml(self.configfilename)["mysql"]))

    def test_get_test_option_files(self):
        test_option_yamls = list_test_yamls("../test_options/")
        for file in test_option_yamls:
            print(pathlib.Path(file).stem)

    def test_read_file_Content(self):
        filecontent = utils.file_utils.read_file_content(
            Constants.MYSQL_DB_SCRIPTS_FILELOC + "get_table_row_count.sql")
        print(filecontent)
