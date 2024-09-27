from ..src.read_yaml import get_yaml
from ..src import validation_engine


class TestFunctions:

    def test_yaml_reader(self):
        yamlout = get_yaml("config")["source_db"]["connection"]
        assert yamlout

    def test_mysql_db_connections(self):
        connection_yaml = get_yaml("config")["source_db"]["connection"]
        db_row_count = validation_engine.get_mmysql_table_row_count(
            connection_yaml=connection_yaml)
        assert len(db_row_count) == 7

    def test_mongodb_document_read(self):
        document_list = validation_engine.get_mongodb_document(get_yaml("config")["target_db"]["connection"], "offices")
        print (len(document_list))
        print (type(document_list))
        print(document_list[0]["officeCode"])
