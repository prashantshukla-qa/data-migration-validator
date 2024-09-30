from ..src.read_yaml import get_yaml
from ..src import database_connector
from ..src.constants import Constants


class TestFunctions:

    configfilename = Constants.DATABASE_CONFIG_FILENAME

    def test_yaml_reader(self):
        yamlout = get_yaml(self.configfilename)["mysql"]
        assert yamlout

    def test_mysql_db_connections(self):
        connection_yaml = get_yaml(self.configfilename)["mysql"]
        db_row_count = database_connector.get_mysql_table_row_count(
            connection_yaml=connection_yaml)
        assert len(db_row_count) == 8

    def test_mongodb_document_read(self):
        document_list = database_connector.get_mongodb_documents(
            get_yaml(self.configfilename)["mongodb"], "offices")
        print(len(document_list))
        print(type(document_list))
        print(document_list[0]["officeCode"])
