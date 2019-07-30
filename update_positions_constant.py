# coding:utf-8
# python2.7

# import sys
# import os
import cx_Oracle
import pandas as pd
# from data import get_price, get_closed_trade_date
# import datetime
# from brinson_verify import get_stock_indu_name_df
# from mfmlib import get_stock_indu_df

# import re


# reload(sys)
# sys.setdefaultencoding("gbk")
# os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.ZHS16GBK'
# db = cx_Oracle.connect('system', 'system', '172.20.100.104:1521/yizhiyuqi')
# db = cx_Oracle.connect('system/system@172.20.100.104:1521/yizhiyuqi')

def read_oracle_csv(sentence):
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


def constant_positions(stdate, eddate, data_port_cp, cstdt):
    del data_port_cp['DEPT']
    del data_port_cp['PORTFOLIO']
    data_port_cp.DATE = data_port_cp.DATE.apply(lambda x: x.strftime('%Y%m%d'))
    positions_df = data_port_cp.groupby(['DATE', 'ASSET_ID']).sum()

    # print(positions_df)
    positions_df.reset_index(['DATE', 'ASSET_ID'], inplace=True)

    # 处理股票列 代码形式
    # def stock_up(str):
    #     str = str.replace(' Equity', '')
    #     str = str.replace(' ', '.')
    #     return str

    def stock_up(str):
        str = str.replace(' Equity', '')
        str = str.replace(' ', '.')
        return str

    positions_df.ASSET_ID = positions_df.ASSET_ID.apply(stock_up)
    # positions_df.to_csv('original.csv')
    # copy
    positions_dfc = positions_df.copy()
    positions_df.set_index(['ASSET_ID'], inplace=True)
    positions_dfc.set_index(['ASSET_ID'], inplace=True)
    dates_ls = positions_dfc.DATE.tolist()
    dates_ls = list(set(dates_ls))
    dates_ls.sort()
    dt_length = len(dates_ls)
    # dt_up = '20181101'
    # dt_up = '20180816'
    dt_up = cstdt

    # dt_up = '20181101'

    date_lc = dates_ls.index(dt_up)
    # print(date_lc)

    # 选取初始状态

    data_pre = positions_dfc[positions_dfc['DATE'] <= dates_ls[date_lc]]

    data_same = positions_dfc[positions_dfc['DATE'] == dates_ls[date_lc]]
    # print(data_same)
    while (date_lc < (len(dates_ls) - 1)):
        date_lc += 1

        data_same['DATE'] = dates_ls[date_lc]

        data_pre = data_pre.append(data_same)

    data_pre.reset_index(['ASSET_ID'], inplace=True)
    data_pre['ASSET_ID'] = data_pre['ASSET_ID'].str.replace('.Limit', '')
    data_pre['PORTFOLIO'] = 'CRAMC'

    UNREALIZED_CNY = data_pre.pop('UNREALIZED_CNY')
    data_pre.insert(4, 'UNREALIZED_CNY', UNREALIZED_CNY)

    # path_save = '专项权益投资部_权益组合持仓汇总' + dt_up + '_' + dates_ls[-1] + '.csv'

    data_pre_s_e = data_pre[(data_pre['DATE'] >= stdate) & (data_pre['DATE'] <= eddate)]

    path_save = 'constant_positions_' + stdate + '_' + eddate + '_' + cstdt + '_.csv'


    data_pre_s_e.to_csv(path_save.decode('utf8'))


if __name__ == '__main__':

    # sentence = "select * from invest_datamart.v_equity_asset_data where LDATE>=to_date('20181231','yyyymmdd')"
    sentence = "select * from invest_datamart.v_equity_asset_data"
    # path_data2csv = 'read_data2csv2.csv'
    #
    data_port = read_oracle_csv(sentence)
    # print(data_port)

    data_port_cp = data_port.copy()

    st = '20180101'
    ed = '20181231'
    cstdt = '20180115'

    constant_positions(st, ed, data_port_cp, cstdt)

    # 603963.SH    b10i    医药

    # print(data_port_cp)
    # del data_port_cp['DEPT']
    # del data_port_cp['PORTFOLIO']
    # data_port_cp.DATE = data_port_cp.DATE.apply(lambda x: x.strftime('%Y%m%d'))
    # positions_df = data_port_cp.groupby(['DATE', 'ASSET_ID']).sum()
    #
    # print(positions_df)
    # positions_df.reset_index(['DATE', 'ASSET_ID'], inplace=True)
    #
    #
    # # 处理股票列 代码形式
    # # def stock_up(str):
    # #     str = str.replace(' Equity', '')
    # #     str = str.replace(' ', '.')
    # #     return str
    #
    #
    # def stock_up(str):
    #     str = str.replace(' Equity', '')
    #     str = str.replace(' ', '.')
    #     return str
    #
    #
    # positions_df.ASSET_ID = positions_df.ASSET_ID.apply(stock_up)
    # # positions_df.to_csv('original.csv')
    # # copy
    # positions_dfc = positions_df.copy()
    # positions_df.set_index(['ASSET_ID'], inplace=True)
    # positions_dfc.set_index(['ASSET_ID'], inplace=True)
    # dates_ls = positions_dfc.DATE.tolist()
    # dates_ls = list(set(dates_ls))
    # dates_ls.sort()
    # dt_length = len(dates_ls)
    # # dt_up = '20181101'
    # # dt_up = '20180816'
    # dt_up = '20171229'
    #
    # # dt_up = '20181101'
    #
    # date_lc = dates_ls.index(dt_up)
    # print(date_lc)
    #
    # # 选取初始状态
    #
    # data_pre = positions_dfc[positions_dfc['DATE']<=dates_ls[date_lc]]
    #
    # data_same = positions_dfc[positions_dfc['DATE']==dates_ls[date_lc]]
    # print(data_same)
    # while (date_lc < (len(dates_ls)-1)):
    #     date_lc += 1
    #
    #     data_same['DATE'] = dates_ls[date_lc]
    #
    #     data_pre = data_pre.append(data_same)
    #
    # data_pre.reset_index(['ASSET_ID'], inplace=True)
    # data_pre['ASSET_ID'] = data_pre['ASSET_ID'].str.replace('.Limit', '')
    # data_pre['PORTFOLIO'] = 'CRAMC'
    #
    # UNREALIZED_CNY = data_pre.pop('UNREALIZED_CNY')
    # data_pre.insert(4, 'UNREALIZED_CNY', UNREALIZED_CNY)
    #
    # # path_save = '专项权益投资部_权益组合持仓汇总' + dt_up + '_' + dates_ls[-1] + '.csv'
    # path_save = dt_up + '_after_same_update.csv'
    # data_pre.to_csv(path_save.decode('utf8'))

