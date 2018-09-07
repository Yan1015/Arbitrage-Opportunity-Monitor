# -*- coding=gbk -*-
import pandas as pd
from WindPy import *
import datetime
import os


w.start() 
trading_day = list(pd.read_csv("trading_day.csv")['trading_day']) 


def windDataTransform(windData):
    pandasData = pd.DataFrame(windData.Data, index=windData.Fields).transpose()
    return pandasData

def currentIndexFutureList():
    #sec_IF = windDataTransform(w.wset("SectorConstituent","sectorId=a599010102000000"))
    sec_IH = windDataTransform(w.wset("SectorConstituent","sectorId=1000014871000000"))
    #sec_IC = windDataTransform(w.wset("SectorConstituent","sectorId=1000014872000000"))
    s = ",".join(list(sec_IH['wind_code']))
    sec_IH['expiredate'] = windDataTransform(w.wsd(s, "lasttrade_date"))['LASTTRADE_DATE'].apply(lambda x: str(x)[:10])
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    sec_IH['expiredate'] = sec_IH['expiredate'].apply(lambda x: trading_day.index(x)-trading_day.index(str(today))) #用交易日重新定义还剩下的天数
    return sec_IH.loc[:, ['wind_code', 'sec_name', 'expiredate']]


def currentOptionList():
    sec_call = windDataTransform(w.wset("OptionChain", "us_code=510050.SH;call_put=call")).loc[:, ['option_code', 'option_name', 'strike_price', 'month', 'expiredate']]
    sec_call = sec_call.assign(call_put='call')
    sec_put = windDataTransform(w.wset("OptionChain", "us_code=510050.SH;call_put=put")).loc[:, ['option_code', 'option_name', 'strike_price', 'month', 'expiredate']]
    sec_put = sec_put.assign(call_put='put')
    r = pd.concat([sec_call, sec_put])
    today = datetime.date.today()
    r['expiredate'] = r['expiredate'].apply(lambda x: trading_day.index(str(today+datetime.timedelta(x)))-trading_day.index(str(today)))
    return r


def optionRealtime(optionList):
    TN = datetime.datetime.now().strftime('%H:%M:%S')
    l = list(optionList['option_code'])
    option_real = windDataTransform(w.wsq(",".join(l), 'rt_pre_settle,rt_last,rt_ask1,rt_asize1,rt_bid1,rt_bsize1,rt_ask2,rt_asize2,rt_bid2,rt_bsize2,rt_ask3,rt_asize3,rt_bid3,rt_bsize3'))
    option_real = option_real.assign(option_code=l)
    r = pd.merge(optionList, option_real, on=['option_code'])
    return r


def indexFutureRealtime(ifList):
    l = list(ifList['wind_code'])
    index_future = windDataTransform(w.wsq(",".join(l), 'rt_pre_settle,rt_last,rt_ask1,rt_asize1,rt_bid1,rt_bsize1'))
    index_future = index_future.assign(wind_code=l)
    r = pd.merge(ifList, index_future, on=['wind_code'])
    return r


def etfAndIndexRealtime():
    etf = windDataTransform(w.wsq("510050.SH", 'rt_pre_close,rt_last')).iloc[0]
    index = windDataTransform(w.wsq("000016.SH", 'rt_pre_close,rt_last')).iloc[0]
    r = {}
    r['etf_pre_close'] = etf['RT_PRE_CLOSE']
    r['etf_last'] = etf['RT_LAST']
    r['sh50_pre_close'] = index['RT_PRE_CLOSE']
    r['sh50_last'] = index['RT_LAST']
    r1 = pd.DataFrame([r])
    return r


def optionTradeVol(optionList):
    l = list(optionList['option_code'])
    r = windDataTransform(w.wsq(",".join(l), 'rt_vol'))
    r['option_code'] = l
    r = pd.merge(optionList, r, on=['option_code'])
    return r

option_list = currentOptionList()
r = optionRealtime(option_list)
r = r.loc[:, ['option_code', 'option_name', 'strike_price', 'month', 'expiredate', 'call_put']]
r['month'] = r['month'].apply(lambda x: 'IH' + str(x)[2:6])
r.columns = ['option_code', 'option_name', 'strike_price', 'future_contract', 'opt_remaining_days', 'call_put']
print r
r.to_csv('info.csv', index=False, encoding='gbk')
