import yfinance as yf
import time
import pandas as pd
import numpy as np
import random
from datetime import datetime
import os

#to be used by compute_prior_year_BS_item_and_average(df, col) to avoid computing twice. Name change requires update of downstream functions
def create_prior_year_BS_item(df, col):
    # df["prior "+ col] = df.groupby("ticker")[col].shift(1)
    prior_col = df.groupby("ticker")[col].shift(1)
    df = pd.concat([df, prior_col.rename("prior " + col)], axis=1)
    return df

#calculates the average between current and previous year balance sheet item
def compute_prior_year_BS_item_and_average(df, col):
    df = create_prior_year_BS_item(df, col)
    df["avg " + col] = (df[col] + df["prior " + col]) / 2
    # df["avg" + col] = df["avg" + col].replace([np.inf, -np.inf], np.nan)
    return df


#Y/Y change (%)
def compute_rate_of_change(df, col):
    new_col = col + " y/y change"
    df[new_col] = df.groupby(by="ticker")[col].pct_change(fill_method=None).replace([np.inf, -np.inf], np.nan)
    return df


def add_rate_of_change_to_df(is_bs_co_df:pd.DataFrame, metrics:list):
    for metric in metrics:
        try:
            is_bs_co_df = compute_prior_year_BS_item_and_average(is_bs_co_df, metric)
            is_bs_co_df = compute_rate_of_change(is_bs_co_df, metric)
        except:
            print(f"error with {metric}")

    return is_bs_co_df


def add_financial_ratio_to_df(is_bs_co_df:pd.DataFrame, financial_ratio_tuples: list):
    for financial_ratio in financial_ratio_tuples:
        try:
            is_bs_co_df = compute_ratio(is_bs_co_df, financial_ratio[0], financial_ratio[1], financial_ratio[2])
        except:
            print(f"error with {financial_ratio[2]}")
    return is_bs_co_df

def retrieve_metric_3Y_time_series(is_bs_co_df:pd.DataFrame, metric:str):
    is_bs_co_df = is_bs_co_df.pivot_table(index='ticker', columns='FY', values=metric)
    is_bs_co_df.columns = [metric +"_"+ col for col in is_bs_co_df.columns]
    is_bs_co_df = is_bs_co_df.iloc[:,-3:]
    return is_bs_co_df

def retrieve_metric_3Y_time_series_wo_metric_name(is_bs_co_df:pd.DataFrame, metric:str):
    is_bs_co_df = is_bs_co_df.pivot_table(index='ticker', columns='FY', values=metric)
    # is_bs_co_df.columns = [metric +"_"+ col for col in is_bs_co_df.columns]
    is_bs_co_df = is_bs_co_df.iloc[:,-3:]
    return is_bs_co_df



def retrieve_metrics_3Y_time_series(is_bs_co_df:pd.DataFrame, ls_metrics_to_show:list, ticker_list:pd.DataFrame,directory_date_metrics_over_time:str):
    dfs_metric_time_series = []

    for metric in ls_metrics_to_show:
        dfs_metric_time_series.append(retrieve_metric_3Y_time_series(is_bs_co_df,metric))

    df_comp = pd.concat(dfs_metric_time_series,axis=1)
    desired_cols = df_comp.columns[:len(ls_metrics_to_show)*3]
    df_comp = df_comp.merge(ticker_list,left_index=True,right_on="Ticker",how="inner")
    df_comp.sort_values(by="sector",inplace=True)
    df_comp = df_comp[["Ticker", "sector"] + list(desired_cols)]

    
            
    if not os.path.exists(directory_date_metrics_over_time):
        os.makedirs(directory_date_metrics_over_time)
        print(f"Directory '{directory_date_metrics_over_time}' created.")
    else:
        print(f"Directory '{directory_date_metrics_over_time}' already exists.")
    
    df_comp.to_csv(f"{directory_date_metrics_over_time}metrics_over_time.csv")

    return df_comp


#computes ratio between two columns 
def compute_ratio(df, col1, col2, ratio_name):
    df[ratio_name] = (df[col1] / df[col2]).replace([np.inf, -np.inf], np.nan)
    return df

def compute_margins():
    pass

def compute_turnovers():
    pass