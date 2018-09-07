# -*- coding=gbk -*-
from DataLoader import *


class StatCalculator:

    def __init__(self, run=True):

        if run==True:
            self.loader = DataLoader()  

            while True:
                t = datetime.datetime.now().strftime("%H:%M:%S")
                if not ((t>"09:30:00" and t<"11:30:00") or (t>"13:00:00" and t<"15:00:00")):
                    print "Not Trading Time, Resting ......"
                else:
                    self.loader.load() 
                    m = self.monitor(self.loader.option_rt, self.loader.index_future_plus_dvd(self.loader.index_future_rt),
                                     self.loader.etf_index_rt) 
                    self.display(m, "Option Complex Monitor") 
                time.sleep(10)

    def cal_premium(self, option_rt, index_future_rt, etf_index_rt):

        etf_pre_settle = etf_index_rt.iloc[0]['ETF_pre_close']
        index_pre_settle = etf_index_rt.iloc[0]['SSE50_pre_close']
        shares = index_pre_settle/etf_pre_settle 
        etf_price = etf_index_rt.iloc[0]['ETF_last']
        index_price = etf_index_rt.iloc[0]['SSE50_last']


        etf_premium = (etf_price*shares-index_price)/index_price
        option = option_rt.loc[:, ['strike_price', 'opt_month', 'opt_remaining_days', 'call_last',
                                   'call_ask1', 'call_asize1', 'call_bid1', 'call_bsize1',
                                   'put_last', 'put_ask1', 'put_asize1', 'put_bid1', 'put_bsize1', 'opt_type']]

        option['opt_parity_premium_ret'] = ((option['call_last']-option['put_last']+option['strike_price'])*shares-index_price)/index_price  # 请参见Black-Scholes平价公式


        option['opt_parity_long_open_cost'] = ((option['call_ask1']-option['call_last'])+(option['put_last']-option['put_bid1']))*shares/index_price
        option['opt_parity_short_open_cost'] = ((option['put_ask1']-option['put_last'])+(option['call_last']-option['call_bid1']))*shares/index_price

     
        option['opt_parity_long_cap'] = option.loc[:, ['call_asize1', 'put_bsize1']].min(1)
        option['opt_parity_short_cap'] = option.loc[:, ['call_bsize1', 'put_asize1']].min(1)
        option['future_contract'] = option['opt_month'].apply(lambda x: "IH170"+str(int(x)))

        option = option.loc[:, ['strike_price', 'future_contract', 'opt_remaining_days', 'opt_parity_premium_ret',
                                'opt_parity_long_open_cost', 'opt_parity_short_open_cost', 'opt_parity_long_cap', 'opt_parity_short_cap', 'opt_type']]
        option['etf_premium_ret'] = etf_premium


        future = index_future_rt.loc[:, 'future_contract':'fut_bsize1']
   
        future['fut_premium_ret'] = (future['fut_last']-index_price)/index_price
        future['fut_long_open_cost'] = (future['fut_ask1']-future['fut_last'])/index_price
        future['fut_short_open_cost'] = (future['fut_last']-future['fut_bid1'])/index_price
        future = future.loc[:, ['future_contract', 'fut_remaining_days', 'fut_premium_ret', 'fut_long_open_cost', 'fut_short_open_cost']]

        return pd.merge(option, future, on='future_contract', how='left')
    

    def cal_cost(self, option_rt, index_future_rt, etf_index_rt):
        etf_last = etf_index_rt.iloc[0]['ETF_last']
        cost = option_rt.loc[:, ['strike_price', 'opt_month']]
        cost['etf_commission_cost'] = (0.0003+0.0003)/etf_last 
        cost['etf_spread_cost'] = 0.002/2/etf_last 
        cost['opt_parity_commission_cost'] = 6.3/etf_last/10000 
        cost['opt_parity_delivery_cost'] = 10/etf_last/10000  
        cost['opt_parity_cost'] = cost['opt_parity_commission_cost']+cost['opt_parity_delivery_cost']+cost['etf_commission_cost']+cost['etf_spread_cost']
        cost['fut_cost'] = 0.0 
        cost['future_contract'] = cost['opt_month'].apply(lambda x: "IH170" + str(int(x)))
        return cost.loc[:, ['strike_price', 'future_contract', 'opt_parity_cost', 'fut_cost']]


    def leverage(self, option_rt, index_future_rt, etf_index_rt):
        option = option_rt.loc[:,
                 ['strike_price', 'opt_month', 'call_pre_settle', 'put_pre_settle', 'call_last', 'put_last']]
        etf_pre_close = etf_index_rt.iloc[0]['ETF_pre_close']

        option['call_open_depo'] = option.apply(lambda x: (
        x['call_pre_settle'] + max(0.12 * etf_pre_close - max(x['strike_price'] - etf_pre_close, 0),
                                   0.07 * etf_pre_close)), axis=1)
        option['put_open_depo'] = option.apply(lambda x: min((x['put_pre_settle'] + max(
            0.12 * etf_pre_close - max(etf_pre_close - x['strike_price'], 0), 0.07 * x['strike_price'])),
                                                             x['strike_price']), axis=1)

        option["opt_parity_short_depo_ratio"] = (option['call_open_depo'] + option['put_last'] - option['call_last']) / \
                                                etf_index_rt.iloc[0]['ETF_last']
        option["opt_parity_long_depo_ratio"] = (option['put_open_depo'] + option['call_last'] - option['put_last']) / \
                                               etf_index_rt.iloc[0]['ETF_last']
        return option.loc[:, ['strike_price', 'opt_month', 'opt_parity_short_depo_ratio', 'opt_parity_long_depo_ratio']]

    def monitor(self, option_rt, index_future_rt, etf_index_rt):
        r = self.cal_premium(option_rt, index_future_rt, etf_index_rt)
        cost = self.cal_cost(option_rt, index_future_rt, etf_index_rt)

        r = pd.merge(r, cost, on=['strike_price', 'future_contract'])

        r['opt_annualized_discount'] = (r['opt_parity_premium_ret']-r['opt_parity_short_open_cost']-r['opt_parity_cost'])/(r['opt_remaining_days'] + 1)*250

        r['fut_annualized_discount'] = (r['fut_premium_ret']-r['fut_short_open_cost']-r['fut_cost'])/(r['fut_remaining_days'] + 1)*250

        r['opt_over_fut'] = (-r['opt_parity_premium_ret']+r['fut_premium_ret']-r['opt_parity_long_open_cost']-r['fut_short_open_cost']-r['opt_parity_cost']-r['fut_cost'])/(r['opt_remaining_days'] + 1)*250
        r['fut_over_opt'] = (r['opt_parity_premium_ret']-r['fut_premium_ret']-r['opt_parity_short_open_cost']-r['fut_long_open_cost']-r['opt_parity_cost']-r['fut_cost'])/(r['opt_remaining_days'] + 1)*250
        r = r.loc[:, ['strike_price', 'future_contract', 'opt_annualized_discount', 'fut_annualized_discount', 'opt_over_fut', 'fut_over_opt', 'opt_parity_long_cap', 'opt_parity_short_cap', 'opt_remaining_days', 'fut_remaining_days', 'opt_type']]
        lvg = self.leverage(option_rt, index_future_rt, etf_index_rt)
        lvg['future_contract'] = lvg['opt_month'].apply(lambda x: "IH170" + str(int(x)))
        lvg = lvg.drop('opt_month', axis=1)

        r = pd.merge(r, lvg, on=['strike_price', 'future_contract'])

        return r
        
    def display(self, dataframe, title):
        dataframe.to_csv('Display/premium.csv', index=False)
        begin = open('Display/display_begin.txt', 'r').read()
        end = open('Display/display_end.txt', 'r').read()
        f = open('Display/' + title + '.html', 'w')
        caption = "<caption>" + title + "</caption>"
        f.write(begin + caption + dataframe.to_html()[36:] + end)
        f.close()

if __name__=='__main__':
    StatCalculator()
