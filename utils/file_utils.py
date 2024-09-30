import glob
import os.path
from utils.constants import Constants


def list_yamls(directory: str) -> list[str]:
    return glob.glob(os.path.join(directory, "*.yaml"))


def list_test_yamls(directory: str) -> list[str]:
    return glob.glob(os.path.join(directory, "test_*.yaml"))


def read_file_content(file_path):
    try:
        if file_path.endswith(".sql"):
            with open(file_path, 'r') as file:
                content = file.read()
            return content
        else:
            raise Exception ("The sql query path name should end with .sql")
    except FileNotFoundError:
        return f"Error: The file '{file_path}' does not exist."
    except Exception as e:
        return f"An error occurred: {e}"
    
def read_mysql_scritps(filename):
    return read_file_content(Constants.MYSQL_DB_SCRIPTS_FILELOC + filename)


# if __name__=="__main__":
#     pass
