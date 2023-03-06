import os
import json
import requests
import pandas as pd
from sandbox.connection import connnect
import datetime as dt
from datetime import timedelta
import numpy as np
import snoop


user_name = 'KMA_STAGE'
passwd = '83tmbF:G'
server = 'nasisexadb-scan'
db_name = 'DKWH'
dsn_1 = str(server + '/' + db_name)
print('DSN NAME:\t', dsn_1)

# api_call_date = str(dt.date.today())  # TODO: Format Date-Month-Year
api_call_date = pd.Timestamp.today()  # TODO: Format Date-Month-Year
print('Record Update Date:\t', api_call_date)


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


def get_df(json_data: any):
    """
    :param json_data: any
    :return: df: data frame
    """
    # print('JSon_Data:\t', pd.json_normalize(json_data))
    # if len(json_data) is not None:
    #     df_from_json = pd.json_normalize(json_data)
    #     print('df_from_Json:\t', df_from_json)
    #     return df_from_json
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
            df_from_json.loc[df_from_json["IS_PRIMARY"] == 1, "IS_PRIMARY"] = 'TRUE'
            df_from_json.loc[df_from_json["IS_PRIMARY"] == 0, "IS_PRIMARY"] = 'FALSE'
            # df_from_json['IS_PRIMARY'] = np.where(df_from_json['IS_PRIMARY'] == 'False', 0, 1)
            # df_from_json['REC_CREATE_DATE'] = pd.Timestamp.today() # .strftime('%Y-%m-%d')
            print('Finale: \tdf_from_json:\t', df_from_json["IS_PRIMARY"])
            return df_from_json
        else:
            print('Json data malfunction!!!')
    except:
        print('Unable to form Data Frame!!!')


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
    # conn = create_connection(user_name, passwd, server, 1521, 'DKWH')
    conn = connnect(user_name, passwd, server, 1521, 'DKWH')
    print('conn:\t', conn)
    print('\nWRiting to DB initiated')
    df.to_sql(name='STG_CORPORATEUSER_TEST', con=conn, schema='KMA_STAGE', if_exists='append', index=False)
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
    #url = "https://stageapimgateway.kdealer.com/COMMONFUNCTION/api" \
    #      "/v1/MSTRUsersFeed/GetActiveCorporateUsers" # API call is not working.
    url = "https://stageapimgateway.kdealer.com/COMMONFUNCTION/api" \
          "/v1/MSTRUsersFeed/GetActiveCorporateUsers"
    data, url_validator = static_url_validator(url)
    json_data = data_load(data, url_validator)
    df = get_df(json_data)
    # df_to_file(df)
    write_to_db(df)
