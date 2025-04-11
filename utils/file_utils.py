import yfinance as yf
import time
import pandas as pd
import numpy as np
import random
from datetime import datetime
import os

#Gets company data for a single ticker and appends it to the all_company_info list.
def retrieve_single_company_info(all_company_info: list,yf_ticker:yf.Ticker, ticker:str):
    try:
        c_dict = yf_ticker.info
        c_dict['ticker'] = ticker
        all_company_info.append(c_dict)
    except:
        print(f"Error fetching data for {ticker}")

def date_today_string():
    return datetime.today().strftime('%Y%m%d')

def load_fin_data_yield(tickers:list, statement:str,date_today:str,directory_statements:str):
    for ticker in tickers:
        file_path = os.path.join(directory_statements,date_today,ticker + "_" + statement + ".csv")
        df = pd.read_csv(file_path)
        df.insert(0,"ticker",ticker)
        df.rename(columns={"Unnamed: 0":"FY_End"},inplace=True)
        yield df