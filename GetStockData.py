import pandas as pd
import requests
from pandas import json_normalize
from datetime import datetime, timedelta
import time
from pandas import DataFrame, concat
import os
from pathlib import Path


def stock_historical_data (symbol, step, countback, end_date):
    global df
    if countback <= 0:
        return
    
    td = int(time.mktime(time.strptime(end_date, "%Y-%m-%d %H:%M:%S")))
    data = requests.get('https://apipubaws.tcbs.com.vn/stock-insight/v2/stock/bars?ticker={}&type=stock&resolution={}&to={}&countBack={}'.format(symbol, step, td, 250)).json()
    df1 = json_normalize(data['data'])
    print(df1.empty)
    
    if df1.empty:
        nextED = td - 60*step
        nextED = datetime.fromtimestamp(nextED)
        stock_historical_data (symbol, step, countback-250, str(nextED))
    else:
        df1['tradingDate'] = pd.to_datetime(df1.tradingDate.str.split(".", expand=True)[0])
        df1.columns = df1.columns.str.title()
        df1.rename(columns={'Tradingdate':'TradingDate'}, inplace=True)
        df1['Code'] = symbol
        df = concat([df1, df], ignore_index=True)
        nextED = df1.iloc[0]['TradingDate'] - timedelta(minutes = step)
        stock_historical_data (symbol, step, countback-250, str(nextED))
        
    return df


def stock_longterm_data (symbol, step):
    td = int(time.time())
    data = requests.get('https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/bars-long-term?ticker={}&type=stock&resolution={}&from=0&to={}'.format(symbol, step, td)).json()
    df1 = json_normalize(data['data'])
    
    df1['tradingDate'] = pd.to_datetime(df1.tradingDate.str.split(".", expand=True)[0])
    df1.columns = df1.columns.str.title()
    df1.rename(columns={'Tradingdate':'TradingDate'}, inplace=True)
    df1['Code'] = symbol
        
    return df1


def printToCSV(df, step, group, code):
    path = os.path.join(step, group)
    Path(path).mkdir(parents=True, exist_ok=True)
    df.to_csv(path + '/' + code + '.csv', sep='\t', mode='w', encoding='utf-8')


# Define stock codes
consumer = ['VNM', 'PLX', 'POW', 'FRT', 'HAX', 'MWG', 'MSN', 'LSS', 'SAB', 'PNJ']
tech = ['FPT', 'ELC', 'SGT', 'ICT', 'ITD', 'SAM', 'CMG', 'CMT', 'TST', 'VGI']
construction = ['HPG', 'PLC', 'NDX', 'S55', 'BCE', 'DC4', 'EVG', 'PHC', 'PTC', 'LCS']
finance = ['STB', 'SSI', 'TCB', 'MBB', 'VND', 'VCB', 'PGI', 'TPB', 'MBS', 'VIG']

# Define stock fields
groupname = ['consumer', 'technology', 'construction', 'finance']
stockcodes = [consumer, tech, construction, finance]


# Get data in a short period (15 min, 30 min or 1 hour)
# for i in range(len(stockcodes)):
#     for j in range(len(stockcodes[i])):
#         df = DataFrame()
#         step = 60 # 15, 30, 60 only
#         df = stock_historical_data(stockcodes[i][j], step, 1600, '2023-05-10 00:00:00')
#         printToCSV(df, str(step), groupname[i], stockcodes[i][j])


# Get data from the first date (Date only)
for i in range(len(stockcodes)):
    for j in range(len(stockcodes[i])):
        step = 'D' # D = Day, W = Week, M = Month
        df = stock_longterm_data(stockcodes[i][j], step)
        printToCSV(df, str(1), groupname[i], stockcodes[i][j])