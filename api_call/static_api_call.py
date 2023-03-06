import os
import json
import time

import requests
import pandas as pd
from sqlalchemy import Integer

from connection import create_connection, create_conn_alter
import datetime as dt
from datetime import datetime
import numpy as np
import snoop
import sqlalchemy
import oracledb


user_name = 'KMA_STAGE'
passwd = '83tmbF:G'
server = 'nasisexadb-scan'
db_name = 'DKWH'
dsn_1 = str(server + '/' + db_name)
print('DSN NAME:\t', dsn_1)

# api_call_date = str(dt.date.today())  # TODO: Format Date-Month-Year
api_call_date = pd.Timestamp.today()  # TODO: Format Date-Month-Year
print('Record Update Date:\t', api_call_date)
today = datetime.now()


@snoop
def static_url_validator(url: str):
    """
    :param url: str
    :return: API_data(json_data)
    """
    url_validator = requests.get(url)

    if url_validator.status_code == 200 and len(url_validator.content) > 0:
        print('Static URL is working...')

    elif url_validator.status_code == 200 and len(url_validator.content) == 0:
        print('URL Validated but Data not received from API!!!')
    else:
        print('API URL is not valid!!!!')

    data = url_validator.content
    # print('Data:\t', data)

    return data, url_validator


@snoop
def data_load(data: any, url_validator: any):
    """
    :param data: any
    :return:
    """
    try:
        json_data = json.loads(data)
    except:
        json_data = json.loads(url_validator.content)
    # print('Json Data:\t', json_data)

    return json_data


@snoop
def get_df(json_data: any):
    """
    :param json_data: any
    :return: df: data frame
    """
    try:
        if len(json_data) is not None:
            df_from_json = pd.json_normalize(json_data)
            print('df_from_Json:\t', df_from_json)
            df_from_json.rename(columns={'userID': 'USER_ID',
                                         'jobTitleID': 'JOBTITLE_ID',
                                         'id': 'ID',
                                         'webDcsID': 'WEB_DCS_ID',
                                         'hierarchyCode': 'HIERARCHY_CODE',
                                         'hierarchyType': 'HIERARCHY_TYPE',
                                         'associationStatus': 'ASSOCIATION_STATUS',
                                         'isPrimary': 'IS_PRIMARY',
                                         }, inplace=True)
            col_ordr = [0, 1, 2, 7, 3, 4, 5, 6]
            df_from_json = df_from_json[df_from_json.columns[col_ordr]]
            if type(df_from_json["IS_PRIMARY"]) != int:
                # df_from_json.loc[df_from_json["IS_PRIMARY"] == 1, "IS_PRIMARY"] = 'TRUE'
                df_from_json.loc[df_from_json["IS_PRIMARY"] == 1, "IS_PRIMARY"] = 1
                # df_from_json.loc[df_from_json["IS_PRIMARY"] == 0, "IS_PRIMARY"] = 'FALSE'
                df_from_json.loc[df_from_json["IS_PRIMARY"] == 0, "IS_PRIMARY"] = 0
            else:
                df_from_json['IS_PRIMARY'] = np.where(df_from_json['IS_PRIMARY'] == 'False', 0, 1)
                df_from_json['IS_PRIMARY'] = np.where(df_from_json['IS_PRIMARY'] == 'True', 1, 0)
            try:
                df_from_json['REC_CREATE_DATE'] = pd.Timestamp("today").strftime("%d-%b-%Y")
            except:
                df_from_json['REC_CREATE_DATE'] = today.strftime("%d-%m-%y")
                pass
            # print('Finale: \tdf_from_json:\t', df_from_json["IS_PRIMARY"])
            return df_from_json
        else:
            print('Json data malfunction!!!')

    except:
        print('Unable to form DataFrame!!!')


@snoop
def write_to_db(df):
    """
    :param df: Data frame
    :return:
    """
    print('DF in WRITE TO DB:\t', df)
    tab_name = "STG_CORPORATEUSER_TEST"
    # df.to_sql(name=table_name, con=db_conn, schema=schema_name, if_exists='append', index=False)
    qry = """select * from KMA_STAGE.STG_CORPORATEUSER_TEST"""
    conn, engine = create_connection(user_name, passwd, server, 1521, 'DKWH')
    # TODO: Truncate table before every load.
    # Truncate table.
    with engine.begin() as conn1:
        trunc_qry = 'Truncate table KMA_STAGE.STG_CORPORATEUSER_TEST'
        trunc = sqlalchemy.text(trunc_qry)
        conn1.execute(sqlalchemy.text(trunc_qry.format(schema="KMA_STAGE")))
        print('Table Truncated !!')
        print('conn1:\t', conn1)
    print('conn:\t', conn)
    print('\nWRiting to DB initiated')
    start_time = time.time()
    use_index = False
    chunk_size = 999 // (len(df.columns) + (1 if use_index else 0))
    df.to_sql(name='STG_CORPORATEUSER_TEST', con=conn,
              schema='KMA_STAGE', index=use_index,
              if_exists='append', method='multi',
              chunksize=chunk_size,
              dtype={"REC_CREATE_DATE": Integer()})
    print("--- %s seconds ---" % (time.time() - start_time))
    print('Execution Successful...')


@snoop
def chunks(l, n):
    """
    :param l: tuple
    :param n: chunk size/ limit.
    :return:
    """
    n = max(1, n)
    return [l[i:i + n] for i in range(0, len(l), n)]


@snoop
def remove_wrong_nulls(x, t):
    """
    :param x: string list
    :param t: tuple
    :return:
    """
    for r in range(len(x)):
        for i,e in enumerate(t):
            for j,k in enumerate(e):
                if k == x[r]:
                    temp=list(t[i])
                    temp[j]=None
                    t[i]=tuple(temp)

@snoop
def write_to_db1(df):
    """
    :param df: dataframe
    :return:
    """
    connect, cursor = create_conn_alter(user_name, passwd, dsn_1)

    # Table truncated.
    cursor.execute('truncate table KMA_STAGE.STG_CORPORATEUSER_TEST')

    # map dataframe values to string and store each row as a tuple in a list of tuples
    for r in df.columns.values:
        df[r] = df[r].map(str)
        df[r] = df[r].map(str.strip)
    tuples = [tuple(x) for x in df.values]

    # Remove None, null, NaT, nan, NaN keyword from Dataframe
    str_list = ['NaT', 'nan', 'NaN', 'None']
    remove_wrong_nulls(str_list, tuples)
    new_list = chunks(tuples, 1000)
    # Insert Query
    ins_req = """ INSERT INTO KMA_STAGE.STG_CORPORATEUSER_TEST ("USER_ID", "JOBTITLE_ID", "ID", "WEB_DCS_ID", "HIERARCHY_CODE", "HIERARCHY_TYPE", "IS_PRIMARY", "ASSOCIATION_STATUS", "REC_CREATE_DATE")  \
              VALUES (:0, :1, :2, :7, :3, :4, :5, :6, :8) """
    print('Write to DB initiated......')
    # print('INS_Req:\t', ins_req)
    # Fetching elements from dataframe to write at target table.
    start_time = time.time()
    for i in range(len(new_list)):
        print('NEW_LIST ITEM:\t', new_list[i])
        cursor.executemany(ins_req, new_list[i])
    connect.commit()
    connect.close()
    end_time = time.time()
    print('Process completion time: \t', (end_time - start_time))

    print('Execution Successful...')


def df_to_file(df):
    """
    :param df: Data frame
    :return: Data frame
    """
    df.to_csv("data.csv", index=False, header=True)
    print('File Creation has been completed...')
    # if df is not None:
    #     df.to_csv("data.csv", index=False, header=True)
    #     print('File Creation has been completed...')
    # else:
    #     print('Data frmae to File conversion not happen!!!')


if __name__ == '__main__':
    # url = "https://stageapimgateway.kdealer.com/COMMONFUNCTION/api" \
    #      "/v1/MSTRUsersFeed/GetActiveCorporateUsers" # API call is not working.
    # url = "https://stageapimgateway.kdealer.com/COMMONFUNCTION/api" \
    #       "/v1/MSTRUsersFeed/GetActiveCorporateUsers"
    url = "https://stageapimgateway.kdealer.com/COMMONFUNCTION/api/v1/MSTRUsersFeed/GetActiveCorporateUsers"
    data, url_validator = static_url_validator(url)
    json_data = data_load(data, url_validator)
    df = get_df(json_data)
    # df_to_file(df)
    # write_to_db(df)
    write_to_db1(df)
