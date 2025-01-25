from typing import Dict
from configparser import ConfigParser


def config(filename: str = "database.ini", section: str = "postgresql") -> Dict[str, str]:
    """Read parameters from database.ini file.

    Args:
        filename (str, optional): Name of file to read data from. Defaults to "database.ini".
        section (str, optional): Name of section inside the file to read data from. Defaults to "postgresql".

    Raises:
        Exception: The section parameter was not detected.

    Returns:
        Dict[str, str]: Name of parameter and its value.
    """
    parser = ConfigParser()
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
        return db
    else:
        raise Exception('Section {0} is not found in the {1} file.'.format(section, filename))
