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


def sell_profit_positions(stdate, eddate, data_port_cp, sldate):
    del data_port_cp['DEPT']
    del data_port_cp['PORTFOLIO']
    data_port_cp.DATE = data_port_cp.DATE.apply(lambda x: x.strftime('%Y%m%d'))
    positions_df = data_port_cp.groupby(['DATE', 'ASSET_ID']).sum()

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
    dt_up = sldate
    # dt_up = '20181101'

    date_lc = dates_ls.index(dt_up)
    print(date_lc)

    # 选取初始状态
    share_holding_st = positions_dfc[
        (positions_dfc['DATE'] == dates_ls[date_lc]) & (positions_dfc['UNREALIZED_CNY'] > 0)]

    # 获取前一个交易日
    trd_dt = get_closed_trade_date(dates_ls[date_lc], fbward=-1)

    # print(trd_dt)

    # 股票列表
    stock_ls = share_holding_st.index.tolist()

    # # 循环股票 # 603590.SH
    for stock in stock_ls:

        stock_holding_variable_st = positions_dfc.ix[
            (positions_dfc['DATE'] == dates_ls[date_lc]) & (positions_dfc.index == stock), 'HOLDING']

        trd_dt = get_closed_trade_date(dates_ls[date_lc], fbward=-1)
        # 前一个交易日股票收盘价 df
        stocks_price = get_price(stock_ls, start_date=trd_dt, end_date=trd_dt)['s_dq_close'].T

        try:
            stock_price = stocks_price.loc[stock, trd_dt]

            # print(stock_price)
            # print(type(stock_price))
            # print(stock_holding_variable_st)
            # print(type(stock_holding_variable_st))
            cash_differ_st = stock_holding_variable_st * stock_price

            cash_differ_st = cash_differ_st.sum()

            positions_dfc.ix[(positions_dfc['DATE'] == dates_ls[date_lc]) & (
                    positions_dfc.index == 'CSH_CNY'), 'HOLDING'] += cash_differ_st

            positions_dfc.ix[
                (positions_dfc['DATE'] == dates_ls[date_lc]) & (
                        positions_dfc.index == stock), 'HOLDING'] = 0

            stock_holding_variable_pre = stock_holding_variable_st.values[0]
            # stock_holding_variable_pre = stock_holding_variable_pre.values
            cash_accumulate = 0
            while (date_lc < (len(dates_ls) - 1)):
                date_lc += 1

                trd_dt = get_closed_trade_date(dates_ls[date_lc], fbward=-1)
                # 前一个交易日股票收盘价 df
                stocks_price = get_price(stock_ls, start_date=trd_dt, end_date=trd_dt)['s_dq_close'].T

                stock_price = stocks_price.loc[stock, trd_dt]

                holding_ori = positions_dfc.ix[
                    (positions_dfc['DATE'] == dates_ls[date_lc]) & (positions_dfc.index == stock), 'HOLDING']

                try:
                    holding_ori = holding_ori.values[0]

                    stock_holding_variable_now = min(stock_holding_variable_pre, holding_ori)

                    cash_accumulate += (stock_holding_variable_pre - stock_holding_variable_now) * stock_price

                    stock_holding_now = holding_ori - stock_holding_variable_now

                    positions_dfc.ix[
                        (positions_dfc['DATE'] == dates_ls[date_lc]) & (
                                positions_dfc.index == 'CSH_CNY'), 'HOLDING'] += (cash_differ_st - cash_accumulate)

                    positions_dfc.ix[
                        (positions_dfc['DATE'] == dates_ls[date_lc]) & (
                                positions_dfc.index == stock), 'HOLDING'] = stock_holding_now

                    # 变量更新
                    stock_holding_variable_pre = stock_holding_variable_now

                except:
                    continue

            date_lc = dates_ls.index(dt_up)


        except:
            continue

    positions_dfc.reset_index(['ASSET_ID'], inplace=True)
    positions_dfc['ASSET_ID'] = positions_dfc['ASSET_ID'].str.replace('.Limit', '')
    positions_dfc['PORTFOLIO'] = 'CRAMC'

    UNREALIZED_CNY = positions_dfc.pop('UNREALIZED_CNY')
    positions_dfc.insert(4, 'UNREALIZED_CNY', UNREALIZED_CNY)

    positions_dfc_s_e = positions_dfc[(positions_dfc['DATE'] >= stdate) & (positions_dfc['DATE'] <= eddate)]


    # path_save = '专项权益投资部_权益组合持仓汇总' + dt_up + '_' + dates_ls[-1] + '.csv'
    path_save = 'sell_profit_positions_' + stdate + '_' + eddate + '_' + sldate + '_.csv'


    positions_dfc_s_e.to_csv(path_save.decode('utf8'))




if __name__ == '__main__':

    # sentence = "select * from invest_datamart.v_equity_asset_data where LDATE>=to_date('20181231','yyyymmdd')"
    sentence = "select * from invest_datamart.v_equity_asset_data"
    # path_data2csv = 'read_data2csv2.csv'
    #
    data_port = read_oracle_csv(sentence)
    # print(data_port)

    data_port_cp = data_port.copy()

    st = '20180101'
    sd = '20181231'
    sldt = '20181101'

    sell_profit_positions(st, sd, data_port_cp, sldt)

    # 603963.SH    b10i    医药

    # print(data_port_cp)
    # del data_port_cp['DEPT']
    # del data_port_cp['PORTFOLIO']
    # data_port_cp.DATE = data_port_cp.DATE.apply(lambda x: x.strftime('%Y%m%d'))
    # positions_df = data_port_cp.groupby(['DATE', 'ASSET_ID']).sum()
    #
    #
    # positions_df.reset_index(['DATE', 'ASSET_ID'], inplace=True)
    #
    #
    # # 处理股票列 代码形式
    # # def stock_up(str):
    # #     str = str.replace(' Equity', '')
    # #     str = str.replace(' ', '.')
    # #     return str
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
    # dt_up = '20180816'
    # # dt_up = '20181101'
    #
    # date_lc = dates_ls.index(dt_up)
    # print(date_lc)
    #
    # # 选取初始状态
    # share_holding_st = positions_dfc[
    #     (positions_dfc['DATE'] == dates_ls[date_lc]) & (positions_dfc['UNREALIZED_CNY'] < 0)]
    #
    # # 获取前一个交易日
    # trd_dt = get_closed_trade_date(dates_ls[date_lc], fbward=-1)
    #
    # # print(trd_dt)
    #
    # # 股票列表
    # stock_ls = share_holding_st.index.tolist()
    #
    # # # 循环股票 # 603590.SH
    # for stock in stock_ls:
    #
    #     stock_holding_variable_st = positions_dfc.ix[
    #         (positions_dfc['DATE'] == dates_ls[date_lc]) & (positions_dfc.index == stock), 'HOLDING']
    #
    #     trd_dt = get_closed_trade_date(dates_ls[date_lc], fbward=-1)
    #     # 前一个交易日股票收盘价 df
    #     stocks_price = get_price(stock_ls, start_date=trd_dt, end_date=trd_dt)['s_dq_close'].T
    #
    #     try:
    #         stock_price = stocks_price.loc[stock, trd_dt]
    #
    #         # print(stock_price)
    #         # print(type(stock_price))
    #         # print(stock_holding_variable_st)
    #         # print(type(stock_holding_variable_st))
    #         cash_differ_st = stock_holding_variable_st * stock_price
    #
    #         cash_differ_st = cash_differ_st.sum()
    #
    #         positions_dfc.ix[(positions_dfc['DATE'] == dates_ls[date_lc]) & (
    #                 positions_dfc.index == 'CSH_CNY'), 'HOLDING'] += cash_differ_st
    #
    #         positions_dfc.ix[
    #             (positions_dfc['DATE'] == dates_ls[date_lc]) & (
    #                     positions_dfc.index == stock), 'HOLDING'] = 0
    #
    #         stock_holding_variable_pre = stock_holding_variable_st.values[0]
    #         # stock_holding_variable_pre = stock_holding_variable_pre.values
    #         cash_accumulate = 0
    #         while (date_lc < (len(dates_ls) - 1)):
    #             date_lc += 1
    #
    #             trd_dt = get_closed_trade_date(dates_ls[date_lc], fbward=-1)
    #             # 前一个交易日股票收盘价 df
    #             stocks_price = get_price(stock_ls, start_date=trd_dt, end_date=trd_dt)['s_dq_close'].T
    #
    #             stock_price = stocks_price.loc[stock, trd_dt]
    #
    #             holding_ori = positions_dfc.ix[
    #                 (positions_dfc['DATE'] == dates_ls[date_lc]) & (positions_dfc.index == stock), 'HOLDING']
    #
    #             try:
    #                 holding_ori = holding_ori.values[0]
    #
    #                 stock_holding_variable_now = min(stock_holding_variable_pre, holding_ori)
    #
    #                 cash_accumulate += (stock_holding_variable_pre - stock_holding_variable_now) * stock_price
    #
    #                 stock_holding_now = holding_ori - stock_holding_variable_now
    #
    #                 positions_dfc.ix[
    #                     (positions_dfc['DATE'] == dates_ls[date_lc]) & (
    #                             positions_dfc.index == 'CSH_CNY'), 'HOLDING'] += (cash_differ_st - cash_accumulate)
    #
    #                 positions_dfc.ix[
    #                     (positions_dfc['DATE'] == dates_ls[date_lc]) & (
    #                             positions_dfc.index == stock), 'HOLDING'] = stock_holding_now
    #
    #                 # 变量更新
    #                 stock_holding_variable_pre = stock_holding_variable_now
    #
    #             except:
    #                 continue
    #
    #         date_lc = dates_ls.index(dt_up)
    #
    #
    #     except:
    #         continue
    #
    # positions_dfc.reset_index(['ASSET_ID'], inplace=True)
    # positions_dfc['ASSET_ID'] = positions_dfc['ASSET_ID'].str.replace('.Limit', '')
    # positions_dfc['PORTFOLIO'] = 'CRAMC'
    #
    # UNREALIZED_CNY = positions_dfc.pop('UNREALIZED_CNY')
    # positions_dfc.insert(4, 'UNREALIZED_CNY', UNREALIZED_CNY)
    #
    # # path_save = '专项权益投资部_权益组合持仓汇总' + dt_up + '_' + dates_ls[-1] + '.csv'
    # path_save = dt_up + '_now_sell_profit_update.csv'
    # positions_dfc.to_csv(path_save.decode('utf8'))

    # stock_price = get_price(stock_ls, start_date=trd_dt, end_date=trd_dt)['s_dq_close'].T
    # print(stock_price)
    # print(stock_price.loc['000001.SZ', trd_dt])
    # print(type(stock_price))
    # print(stock_price.columns)
    # print(stock_price.index)

