# coding:utf-8
# python2.7

# import sys
# import os
import cx_Oracle
import pandas as pd
# from brinson_verify import get_stock_indu_name_df
from dquant.mfmlib import get_stock_indu_df
import matplotlib.pyplot as plt

# import matplotlib.pyplot as plt

# reload(sys)
# sys.setdefaultencoding("gbk")
# os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.ZHS16GBK'
# db = cx_Oracle.connect('system', 'system', '172.20.100.104:1521/yizhiyuqi')
# db = cx_Oracle.connect('system/system@172.20.100.104:1521/yizhiyuqi')

def read_oracle_csv(sentence):

    db = cx_Oracle.connect('ASSET_REPORT/ASSET_REPORT_2018@172.20.0.57:1521/DPFPROD')
    cursor = db.cursor()
    # try:
    #     # cursor.parse("select * SCHEDULER_PROGRAM_ARGS")
    #     cursor.parse("select * invest_datamart.v_equity_asset_data")
    # except cx_Oracle.DatabaseError as e:
    #     print(e)
    cursor.execute(sentence)
    # row = cursor.fetchone()
    row = cursor.fetchall()
    # row = cursor.fetchmany(2000)
    # print(row)
    # print(type(row))
    #
    cursor.close()
    db.close()

    # print(row)


    cols = ['DATE', 'ASSET_ID', 'HOLDING', 'PORTFOLIO', 'DEPT','UNREALIZED_CNY']
    df = pd.DataFrame(row,columns=cols)

    
    return df
    # df.to_csv(path_save1)



def loss_sum_indu_plot_f(df, date_st, date_ed, field):
    
        
    

    
    data_port_cp = df    
    del data_port_cp['DEPT']
    del data_port_cp['PORTFOLIO']
    data_port_cp.DATE = data_port_cp.DATE.apply(lambda x: x.strftime('%Y%m%d'))
    df_g = data_port_cp.groupby(['DATE', 'ASSET_ID']).sum()



    df_g.reset_index(['DATE', 'ASSET_ID'], inplace=True)
        
    

    def stock_up(str):
        str = str.replace(' Equity', '')
        str = str.replace(' ', '.')
        return str
    
    
    

    df_g.ASSET_ID = df_g.ASSET_ID.apply(stock_up)
    
    # 这里要先去掉后缀.Limit
    df_g['ASSET_ID'] = df_g['ASSET_ID'].str.replace('.Limit', '')

    data_calc = df_g[(df_g['DATE'] <= date_ed) & (df_g['DATE'] >= date_st)]
    date_ls = data_calc['DATE'].tolist()

    date_ls = list(set(date_ls))
    date_ls.sort()

    stock_ind = get_stock_indu_df(date_st)
    # print(stock_ind)
    stock_ind.reset_index(inplace=True)

    stock_ind_dict = stock_ind.set_index('s_info_windcode').to_dict()['indu']


    def stock2indu(stock):
        return stock_ind_dict.get(stock)

    df_g['indu'] = df_g.ASSET_ID.apply(stock2indu)

#     print(df_g)
    array_ = []
    for dttime in date_ls:

        df_dttime = df_g[df_g['DATE'] == dttime]

        sr_dttime = df_dttime.ix[(df_dttime.UNREALIZED_CNY<=0) & (df_dttime.indu.isin(field)),'UNREALIZED_CNY']
        profit_b10l_sum = sr_dttime.sum()
        new_row = [dttime, profit_b10l_sum]
        array_.append(new_row)
    
    name_part = '_'.join(field)
    
    
    
    profit_name = 'loss_' + name_part + '_sum' 

    profit__sum_df = pd.DataFrame(array_, columns=['date', profit_name])

    # profit_b10l_sum_df.set_index(['date'], inplace=True)
    # profit_b10l_sum_df.sort_index(inplace=True)
    # print(profit__sum_df)
    path_save = profit_name + '_' + date_st + '_' + date_ed
    
    fig = plt.figure()
    # profit_b10l_b10n_b10m_sum_df = profit_b10l_sum_df.copy()
    profit__sum_df.plot(figsize=(20, 10), grid=True)
    ax = plt.gca()
    ax = plt.axes()
    ax.set_xticks(profit__sum_df.index[1:-1:8])
    ax.set_xticklabels(profit__sum_df.date[1:-1:8], rotation=30)
    
    # plt.savefig('profit_b10l_sum.pdf')
    plt.savefig(path_save + '.png')
    profit__sum_df.to_csv(path_save + '.csv')
    # plt.show()
    
    


if __name__ == '__main__':
    # sentence = "select * from invest_datamart.v_equity_asset_data where LDATE>=to_date('20181231','yyyymmdd')"
    sentence = "select * from invest_datamart.v_equity_asset_data"
    # path_data2csv = 'read_data2csv2.csv'
    #
    data_port = read_oracle_csv(sentence)
    # print(data_port)

    data_port_cp = data_port.copy()

    # 603963.SH    b10i    医药
    
    field = ['b10m']

    # print(data_port_cp)
   


    

    # df_g.to_csv('original.csv')
#     date_st = '20180115'
    date_st = '20180101'
    date_ed = '20181231'
    
    loss_sum_indu_plot_f(data_port_cp, date_st, date_ed, field)
    
    
    
    
    # 处理股票列 代码形式        # .Limit暂不考虑