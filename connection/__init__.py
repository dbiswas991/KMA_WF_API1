import os
import sys
import cx_Oracle as oracledb
from sqlalchemy import create_engine, event
import getpass
# import oracledb
import snoop


connection_type = 'RDBMS'
user_name = 'KMA_STAGE'
pwd = '83tmbF:G'
server = 'nasisexadb-scan'
port = 1521
db_name = 'DKWH'
dsn = str(server+"/"+db_name)
# print('dsn:\t', dsn)

#oracledb.init_oracle_client()

@snoop
def create_connection(username:str,pwd:str,server:str,port:int,db_name:str):
    """
    :param username:str
    :param pwd: str
    :param server: str
    :param port: int
    :param db_name: str
    :return:
    """
    # conn_str = "oracle+cx_oracle://" + user_name + ":" + pwd + "@" + server + ":" + str(port) + "/?service_name=" + db_name
    conn_str = u"oracle+cx_oracle://" + username + ":" + pwd + "@" + server + ":" + str(port) + "/" + db_name

    engine = create_engine(conn_str)
    print('conn_str:\t', conn_str, '\nengine:\t', engine)
    conn = engine.connect()
    print('connection test:\t', conn)
    print('Connection Successful>>>>')
    # cursor = conn.cursor
    return conn, engine


def create_conn_alter(user_name,pwd,dsn):
    connect = oracledb.connect(user=user_name,
                               password=pwd,
                               dsn=dsn)
    print('connection test:\t', connect)
    print('Connection Successful>>>>')
    cursor = connect.cursor()
    return connect, cursor


if __name__ == '__main__':
    create_connection(user_name,pwd,server,port,db_name)
    # create_conn_alter(user_name,pwd,dsn)
