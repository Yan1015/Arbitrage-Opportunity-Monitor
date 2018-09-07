# -*- coding=gbk -*-
import time
import pandas as pd
import numpy as np
import datetime
import ConfigParser
import time
import cx_Oracle
import os

pd.options.display.width = 500

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

config = ConfigParser.ConfigParser()
config.read("config.ini")

username = config.get("sysconfig", "username")
password = config.get("sysconfig", "password")
url = config.get("sysconfig", "url")

#连接oracle数据库
oracle_conn = None
try:
    oracle_conn = cx_Oracle.connect('%s/%s@%s' % (username, password, url))
except BaseException as e:
    print e
    raise Exception("连接【%s/%s】配置库异常，请确认后重试" % (url, username))
finally:
    del username, password, url


class DataLoader:

    def __init__(self):
        # self.option_list = currentOptionList()
        # self.future_list = currentIndexFutureList()
        self.trading_day = list(pd.read_csv("trading_day.csv")['trading_day'])
        self.today = datetime.datetime.now().strftime("%Y-%m-%d")

        self.option_rt = None
        self.index_future_rt = None
        self.index_future_dvd_rt = None
        self.etf_index_rt = None

    def option_realtime(self):
        call_columns = ['wind_code', 'strike_price', 'opt_month', 'opt_remaining_days', 'call_pre_settle', 'call_last', 'call_ask1', 'call_asize1',\
                        'call_bid1', 'call_bsize1', 'call_ask2', 'call_asize2', 'call_bid2', 'call_bsize2', 'call_ask3', 'call_asize3',\
                        'call_bid3', 'call_bsize3']
        call = pd.DataFrame(np.random.randn(1, len(call_columns)), columns=call_columns, index=[0]).drop([0])
        put_columns = ['strike_price', 'opt_month', 'put_pre_settle', 'put_last', 'put_ask1', 'put_asize1', 'put_bid1', 'put_bsize1',\
                       'put_ask2', 'put_asize2', 'put_bid2', 'put_bsize2', 'put_ask3', 'put_asize3', 'put_bid3', 'put_bsize3']
        put = pd.DataFrame(np.random.randn(1, len(put_columns)), columns=put_columns, index=[0]).drop([0])

        #option

        current_date = time.strftime("%Y%m%d", time.localtime())
        sql = "WITH CC AS(SELECT TRADINGDATE,INSTRUMENTID,LASTPRICE,BIDPRICE1,BIDVOLUME1,BIDPRICE2,BIDVOLUME2,BIDPRICE3,BIDVOLUME3,ASKPRICE1,ASKVOLUME1,ASKPRICE2,ASKVOLUME2,ASKPRICE3,ASKVOLUME3,ROW_NUMBER()OVER(PARTITION BY INSTRUMENTID ORDER BY TRADINGDATE DESC) AS ORDER_ID FROM WAREHOUS1.OPTION_DETAIL_%s WHERE TRADINGDATE < TO_DATE(TO_CHAR(SYSDATE, 'YYYYMMDD')||'160000', 'YYYYMMDDHH24MISS')),C0 AS(SELECT INSTRUMENTID,INSTRUMENTNAME,EXPIREDATE,CEIL(EXPIREDATE-SYSDATE)+1 REMAININGDAYS,DELIVERYMONTH,EXECPRICE,PRESETTLEMENTPRICE FROM SYSTEM_SUBSCRIBE_MARKET WHERE MARKETTYPE='option') SELECT CC.*,INSTRUMENTNAME,EXPIREDATE,REMAININGDAYS,DELIVERYMONTH,EXECPRICE,PRESETTLEMENTPRICE FROM CC INNER JOIN C0 ON C0.INSTRUMENTID=CC.INSTRUMENTID WHERE CC.ORDER_ID=1" % current_date
        query_cursor = oracle_conn.cursor()
        query_cursor.execute(sql)
        rows = query_cursor.fetchall()
        call_num = 0
        put_num = 0
        for option_item in rows:
            if not option_item[21]:
                continue

            if u'购' in option_item[16].decode('utf-8'):
                call.loc[call_num, 'wind_code'] = option_item[1]
                call.loc[call_num, 'strike_price'] = option_item[20]
                call.loc[call_num, 'opt_month'] = option_item[19]
                call.loc[call_num, 'opt_remaining_days'] = self.trading_day.index(str(option_item[17].strftime("%Y-%m-%d"))) - self.trading_day.index(str(self.today))
                call.loc[call_num, 'opt_name_call'] = option_item[16].decode('utf-8').strip()

                if len(option_item[16].decode('utf-8').strip()) != 13:
                    call.loc[call_num, 'opt_type'] = 'M'
                else:
                    call.loc[call_num, 'opt_type'] = option_item[16].decode('utf-8').strip()[12]

                call.loc[call_num, 'call_pre_settle'] = option_item[21]
                call.loc[call_num, 'call_last'] = option_item[2]
                call.loc[call_num, 'call_ask1'] = option_item[9]
                call.loc[call_num, 'call_asize1'] = option_item[10]
                call.loc[call_num, 'call_bid1'] = option_item[3]
                call.loc[call_num, 'call_bsize1'] = option_item[4]
                call.loc[call_num, 'call_ask2'] = option_item[11]
                call.loc[call_num, 'call_asize2'] = option_item[12]
                call.loc[call_num, 'call_bid2'] = option_item[5]
                call.loc[call_num, 'call_bsize2'] = option_item[6]
                call.loc[call_num, 'call_ask3'] = option_item[13]
                call.loc[call_num, 'call_asize3'] = option_item[14]
                call.loc[call_num, 'call_bid3'] = option_item[7]
                call.loc[call_num, 'call_bsize3'] = option_item[8]
                call_num = call_num + 1
            if u'沽' in option_item[16].decode('utf-8'):

                put.loc[put_num, 'opt_name_put'] = option_item[16].decode('utf-8').strip()
                if len(option_item[16].decode('utf-8').strip()) != 13:
                    put.loc[put_num, 'opt_type'] = 'M'
                else:
                    put.loc[put_num, 'opt_type'] = option_item[16].decode('utf-8').strip()[12]
                put.loc[put_num, 'strike_price'] = option_item[20]
                put.loc[put_num, 'opt_month'] = option_item[19]
                put.loc[put_num, 'put_pre_settle'] = option_item[21]
                put.loc[put_num, 'put_last'] = option_item[2]
                put.loc[put_num, 'put_ask1'] = option_item[9]
                put.loc[put_num, 'put_asize1'] = option_item[10]
                put.loc[put_num, 'put_bid1'] = option_item[3]
                put.loc[put_num, 'put_bsize1'] = option_item[4]
                put.loc[put_num, 'put_ask2'] = option_item[11]
                put.loc[put_num, 'put_asize2'] = option_item[12]
                put.loc[put_num, 'put_bid2'] = option_item[5]
                put.loc[put_num, 'put_bsize2'] = option_item[6]
                put.loc[put_num, 'put_ask3'] = option_item[13]
                put.loc[put_num, 'put_asize3'] = option_item[14]
                put.loc[put_num, 'put_bid3'] = option_item[7]
                put.loc[put_num, 'put_bsize3'] = option_item[8]
                put_num = put_num + 1



        r = pd.merge(call, put, on=['strike_price', 'opt_month', 'opt_type'])
        r = r.sort_values(by = ['opt_month', 'strike_price'])
        r.index = range(len(r))

        return r



    def index_future_realtime(self):
        index_future_columns = ['wind_code', 'future_contract', 'fut_remaining_days', 'fut_pre_settle', 'fut_last', 'fut_ask1', 'fut_asize1', 'fut_bid1', 'fut_bsize1']
        index_future = pd.DataFrame(np.random.randn(1, len(index_future_columns)), columns=index_future_columns, index=[0]).drop([0])

        current_date = time.strftime("%Y%m%d", time.localtime())
        sql = "WITH CC AS(SELECT TRADINGDATE,INSTRUMENTID,LASTPRICE,BIDPRICE1,BIDVOLUME1,ASKPRICE1,ASKVOLUME1,ROW_NUMBER()OVER(PARTITION BY INSTRUMENTID ORDER BY TRADINGDATE DESC) AS ORDER_ID FROM WAREHOUS1.FUTURE_DETAIL_%s WHERE TRADINGDATE < TO_DATE(TO_CHAR(SYSDATE, 'YYYYMMDD')||'160000', 'YYYYMMDDHH24MISS')),C0 AS(SELECT INSTRUMENTID,INSTRUMENTNAME,EXPIREDATE,CEIL(EXPIREDATE-SYSDATE)+1 REMAININGDAYS,DELIVERYMONTH,EXECPRICE,PRESETTLEMENTPRICE FROM SYSTEM_SUBSCRIBE_MARKET WHERE MARKETTYPE='future')SELECT CC.*,INSTRUMENTNAME,EXPIREDATE,REMAININGDAYS,DELIVERYMONTH,EXECPRICE,PRESETTLEMENTPRICE FROM CC INNER JOIN C0 ON C0.INSTRUMENTID=CC.INSTRUMENTID WHERE CC.ORDER_ID=1" % current_date
        query_cursor = oracle_conn.cursor()
        query_cursor.execute(sql)
        rows = query_cursor.fetchall()
        index_future_num = 0
        for future_item in rows:
            if 'IH' in str(future_item[1]):

                index_future.loc[index_future_num, 'wind_code'] = future_item[1]
                index_future.loc[index_future_num, 'future_contract'] = "IH170" + str(int(future_item[11]))
                index_future.loc[index_future_num, 'fut_remaining_days'] = self.trading_day.index(str(future_item[9].strftime("%Y-%m-%d"))) - self.trading_day.index(str(self.today))
                index_future.loc[index_future_num, 'fut_pre_settle'] = future_item[13]
                index_future.loc[index_future_num, 'fut_last'] = future_item[2]
                index_future.loc[index_future_num, 'fut_ask1'] = future_item[5]
                index_future.loc[index_future_num, 'fut_asize1'] = future_item[6]
                index_future.loc[index_future_num, 'fut_bid1'] = future_item[3]
                index_future.loc[index_future_num, 'fut_bsize1'] = future_item[4]
                index_future_num = index_future_num + 1


        return index_future

    def index_future_plus_dvd(self, index_future_rt):
        dvd_arr = [0, 0, 0, 0, 0]
        dvd = []
        dvd.append({'future_contract':'IH1704', 'dividend':sum(dvd_arr[:1])})
        dvd.append({'future_contract':'IH1705', 'dividend':sum(dvd_arr[:2])})
        dvd.append({'future_contract':'IH1706', 'dividend':sum(dvd_arr[:-1])})
        dvd.append({'future_contract':'IH1709', 'dividend':sum(dvd_arr[:])})
        dvd = pd.DataFrame(dvd)
        r = index_future_rt
        r = pd.merge(r, dvd, on=['future_contract'])
        r['fut_pre_settle'] += r['dividend']
        r['fut_last'] += r['dividend']
        r['fut_ask1'] += r['dividend']
        r['fut_bid1'] += r['dividend']

        return r

    def etf_and_index_realtime(self):

        current_date = time.strftime("%Y%m%d", time.localtime())
        sql = "WITH CC AS(SELECT TRADINGDATE,INSTRUMENTID,LASTPRICE,BIDPRICE1,BIDVOLUME1,BIDPRICE2,BIDVOLUME2,BIDPRICE3,BIDVOLUME3,ASKPRICE1,ASKVOLUME1,ASKPRICE2,ASKVOLUME2,ASKPRICE3,ASKVOLUME3,ROW_NUMBER()OVER(PARTITION BY INSTRUMENTID ORDER BY TRADINGDATE DESC) AS ORDER_ID FROM WAREHOUS1.OPTION_DETAIL_%s WHERE TRADINGDATE < TO_DATE(TO_CHAR(SYSDATE, 'YYYYMMDD')||'160000', 'YYYYMMDDHH24MISS')),C0 AS(SELECT INSTRUMENTID,INSTRUMENTNAME,EXPIREDATE,CEIL(EXPIREDATE-SYSDATE)+1 REMAININGDAYS,DELIVERYMONTH,EXECPRICE,PRESETTLEMENTPRICE FROM SYSTEM_SUBSCRIBE_MARKET WHERE MARKETTYPE='option') SELECT CC.*,INSTRUMENTNAME,EXPIREDATE,REMAININGDAYS,DELIVERYMONTH,EXECPRICE,PRESETTLEMENTPRICE FROM CC INNER JOIN C0 ON C0.INSTRUMENTID=CC.INSTRUMENTID WHERE CC.ORDER_ID=1" % current_date
        query_cursor = oracle_conn.cursor()
        query_cursor.execute(sql)
        rows = query_cursor.fetchall()
        s = {}
        for item in rows:
            if item[16].decode('utf-8').strip() == u'上证50':
                s['SSE50_last'] = round(item[2], 2)
                s['SSE50_pre_close'] = 2355.5
            if item[16].strip() == '50ETF':
                s['ETF_last'] = round(item[2], 3)
                s['ETF_pre_close'] = 2.354

        return pd.DataFrame([s])

    def load(self):
        cur_date = datetime.datetime.now().strftime("%Y-%m-%d")
        cur_time = datetime.datetime.now().strftime("%H:%M:%S")
        self.option_rt = self.option_realtime().assign(time=cur_time)
        self.index_future_rt = self.index_future_realtime().assign(time=cur_time)
        self.index_future_dvd_rt = self.index_future_plus_dvd(self.index_future_rt)
        self.etf_index_rt = self.etf_and_index_realtime().assign(time=cur_time)
        if not os.path.exists("DATA/"+cur_date):
            os.mkdir("DATA/"+cur_date)
            self.option_rt.to_csv("DATA/"+cur_date+"/option_realtime.csv", index=False, encoding='gbk')
            self.index_future_rt.to_csv("DATA/"+cur_date+"/index_future_realtime.csv", index=False)
            self.etf_index_rt.to_csv("DATA/"+cur_date+"/etf_index_realtime.csv", index=False)
        else:
            fo = open("DATA/"+cur_date+"/option_realtime.csv", 'a')
            fi = open("DATA/"+cur_date+"/index_future_realtime.csv", 'a')
            fe = open("DATA/"+cur_date+"/etf_index_realtime.csv", 'a')
            self.option_rt.to_csv(fo, index=False, header=False, encoding='gbk')
            self.index_future_rt.to_csv(fi, index=False, header=False)
            self.etf_index_rt.to_csv(fe, index=False, header=False)
            fo.close()
            fi.close()
            fe.close()

