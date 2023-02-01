import pyodbc
import configparser

import constants


def get_sql_connection():
    """
    This method simply returns the sql connection based on the DB type and the connection settings
    defined in the db.conf
    :return: connection
    """

    # Reading the Database configurations
    db_config = configparser.ConfigParser()
    db_config.read(constants.ROOT_DIR + constants.DB_CONFIG)
    db_type = db_config['SYSTEM']['db_type']
    server = db_config[db_type]['server']
    database = db_config[db_type]['database']
    driver = db_config[db_type]['driver']
    username = db_config[db_type]['username']
    password = db_config[db_type]['password']
    print(username)
    return pyodbc.connect(
            r'Driver=' + driver +';Server=' + server + ';Database=' + database +';Encrypt=no'+ ';Trusted_Connection=no;'+'UID='+username+';PWD='+password+';')


def close_sql_connection(connection):
    """
    Take care of the closing process of the SQL connection
    :param connection: sql_connection
    :return: operation status
    """
    return connection.close()
