# coding:utf-8
# python2.7

# import sys
# import os
import cx_Oracle
import pandas as pd
from dquant.data import get_price, get_closed_trade_date
# import datetime
# from brinson_verify import get_stock_indu_name_df
# from mfmlib import get_stock_indu_df

import re


# reload(sys)
# sys.setdefaultencoding("gbk")
# os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.ZHS16GBK'
# db = cx_Oracle.connect('system', 'system', '172.20.100.104:1521/yizhiyuqi')
# db = cx_Oracle.connect('system/system@172.20.100.104:1521/yizhiyuqi')

def positions_all(sentence):
    db = cx_Oracle.connect('ASSET_REPORT/ASSET_REPORT_2018@172.20.0.57:1521/DPFPROD')
    cursor = db.cursor()
    cursor.execute(sentence)
    # row = cursor.fetchone()
    row = cursor.fetchall()

    cursor.close()
    db.close()
    cols = ['DATE', 'ASSET_ID', 'HOLDING', 'PORTFOLIO', 'DEPT', 'UNREALIZED_CNY']
    df = pd.DataFrame(row, columns=cols)

    # print(df)
    # positions_dfc = df.groupby(['DATE', 'ASSET_ID']).sum()
    # positions_dfc.reset_index('ASSET_ID', inplace=True)
    
    
    
    return df
    # df.to_csv(path_save1)
    


def positions_get(stdate, eddate, df):
    
    positions_df = df[(df['DATE'] <= eddate) & (df['DATE'] >= stdate)]
    
    path_save = 'positions_' + stdate + '_' + eddate + '_.csv'
    
    positions_df.to_csv(path_save.decode('utf8'))
    


if __name__ == '__main__':

    # sentence = "select * from invest_datamart.v_equity_asset_data where LDATE>=to_date('20181231','yyyymmdd')"
    sentence = "select * from invest_datamart.v_equity_asset_data"
    
    
    data_port = positions_all(sentence)
    
    stdate = '20171229'
    eddate = '20181231'
    
    positions_get(stdate, eddate, data_port)
    
    
    
    
    