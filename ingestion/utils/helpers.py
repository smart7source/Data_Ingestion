import getpass
import json
import logging
import traceback
from pathlib import Path

def create_dir(dir_path):
    """
    Creates directory from given path
    :param dir_path: relative path of directory to create
    :return: N/A
    """
    try:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
    except Exception as ex:
        msg = f"Error creating directory from given relative path {dir_path}"
        log_exception_details(message=msg, exception_object=ex)
        raise ex


def get_user():
    """
    Fetches username of the executor

    Args:

    Returns:
        :return: username of the executor / logged in machine
    """
    return getpass.getuser()


def is_null_or_empty(obj) -> bool:
    """
    Checks if an object is null or empty if object is of type string

    Args:
        :param obj: object / variable to validate

    Returns:
        :return: bool True of object is null or string is empty False otherwise
    """
    if obj is None:
        return True
    elif type(obj) is str and str(obj).strip().__eq__(''):
        return True
    else:
        return False


def get_project_root() -> Path:
    """
    Identifies project root, Returns project root, the repository root
    Args:

    Returns:
        :return: project's root path as type Path
    """
    return Path(__file__).parent.parent.parent.parent.parent


def read_json_get_dict(json_path) -> dict:
    """
    Reads json file from given `json_path` & returns as python dict
    Args:
        :param :json_path : Absolute or Relative path of json file to read & convert

    Return:
        :return :json_as_dict: JSON content as dictionary type
    """
    print("Here is the JSON File Path.......................")
    print(json_path)
    try:
        with open(json_path, 'r') as stream:
            json_as_dict = json.load(stream)
        stream.close()
        return json_as_dict
    except Exception as ex:
        log_exception_details(f'Error reading json file {json_path}, error traceback below', ex)


def log_exception_details(message, exception_object):
    """
    Logs the exception to console & log file for every exception

    Args:
        :param message: Developer's message on exception
        :param exception_object: Class object of the exception

    Returns: N/A
    """
    print(exception_object.__str__())
    print(traceback.format_exc())
    print(message)
