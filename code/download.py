# -*- coding: utf-8 -*-
'''
公司：中融汇信
作者：王德扬
创建时间：2017/07/24
目的：生成每日的期货交易量数据
注释：
    wind成交额的计算方式
    zj 单边
    sq 双边
    ds 双边
    zs 双边
'''

import os
import re
import pandas as pd
from datetime import datetime
from WindPy import *

global persym;persym=dict()
global ctrs_data;ctrs_data=dict() #存储本次下载的所有信息
global sym_index;sym_index=dict()
global mtpr;mtpr=dict()
global ratio;ratio=dict()
__all__=['getctrs','download','get_mtpr','load_mtpr','fix_mtpr','roll','merge','download_all',
         'merge_all','to_disk','merge2','merge_all2','to_disk2','get_ratio','merge3','merge_all3']

# 初始化
def init():
    '''
    @note:初始化函数,生成对应文件夹
    '''
    w.start()
    if not os.path.exists('../raw'):
        os.mkdir('../raw')
    if not os.path.exists('../fig'):
        os.mkdir('../fig')
    if not os.path.exists('../data'):
        os.mkdir('../data')

def getctrs():
    '''
    @note:生成所有的wind合约名
    '''
    with open("./ctrlist.txt") as f:
        for line in f.readlines():
            yield line.strip()

def download(ctr,sym): 
    '''
    @note:下载所有的合约相关数据,包括收盘，成交，总量，持仓，结算
    @ctr:合约名
    @sym:品种名
    '''
    today=datetime.today()
    a=w.wsd(ctr, "close,volume,amt,oi,settle", "2008-01-01", f"{today.year}-{today.month}-{today.day}", "")#收盘 成交 总量 持仓 结算
    df=pd.DataFrame(a.Data,index=a.Fields,columns=a.Times).T # 转换成pd格式
    df.to_csv(f"../raw/{ctr}.csv") #存储到本地
    #df=pd.DataFrame.from_csv(f"../raw/{ctr}.csv") #测试时使用
    if sym not in ctrs_data:
        ctrs_data[sym]=dict()    
    ctrs_data[sym][ctr]=df.copy() #存储到ctrs_data
    sym_index[sym]=df.index

def merge(sym,ctrs): 
    '''
    @note:合并单品种的成交量
    只有中金所是单边计算 其余均为双边计算
    '''
    global persym
    df=pd.concat([ctr['VOLUME']*ctr['SETTLE']*get_mtpr(sym) for ctr in ctrs.values()],axis=1)
    if sym not in ['IF','IH','IC','TF','T']: #只有中金所是单边计算 其余均为双边计算
        df=df/2
    persym[sym]=df.sum(1) #按日合并数据

def download_all(): 
    '''
    @note:下载所有品种的量价数据 
    '''
    for ctr in getctrs():
        sym=re.search('(\D+)',ctr).group(0)
        download(ctr,sym)

def merge_all():
    '''
    @note:分品种合成成交量数据
    '''
    for sym in ctrs_data.keys():
        merge(sym,ctrs_data[sym])

def to_disk():
    '''
    @note:储存各品种成交量数据，以及每日成交总额
    '''
    global persym
    df=pd.concat([pd.DataFrame(x) for x in persym.values()],axis=1)
    df.columns=persym.keys()
    df.to_csv("../data/trade.csv")    
    df.sum(1).to_csv("../data/tradeall.csv") #按行合并计算每日成交额
    
def get_ratio(sym):
    '''
    @note:提取品种保证金数据
    '''
    global ratio
    return ratio[str.upper(sym)]

if __name__=="__main__":
    init() 
    download_all()
    load_mtpr()
    merge_all()
    to_disk()
    merge_all2()
    to_disk2()
    merge_all3()
#    import download
#    print(help(download))
    