# 전처리

import pandas as pd
import numpy as np
from scipy import stats

def drop_duplicates_in_list(lst):
    return list(set(lst))

def get_pred_data():

    # load all raw data
    sales = pd.read_csv('raw_data/sale/movie_sale.csv')
    summary = pd.read_csv('raw_data/summary/movie_summary.csv')
    genre = pd.read_csv('raw_data/genre/movie_genre.csv')

    # only choose columns we need
    sales_df = sales[['date','movieCd', 'movieNm', 'salesAmt', 'salesShare', 'scrnCnt', 'showCnt', 'audiCnt']]
    sales_df2 = sales_df[(np.abs(stats.zscore(sales_df[['audiCnt', 'salesAmt', 'movieCd']])) < 2).all(axis=1)]

    # Join the tables based on the movieCd
    merged_df = pd.merge(sales_df2, genre, on='movieCd', how='inner')

    # Group by movieCd and aggregate genres into a list
    grouped_df = merged_df.groupby('movieCd')['genres'].apply(list).reset_index(name='genres')
    grouped_df['genres'] = grouped_df['genres'].apply(drop_duplicates_in_list)

    # Merge the grouped_df back to movie_sales_df based on movie ID
    final_df = pd.merge(sales_df2, grouped_df, on='movieCd', how='left')

    # Convert the genres column to a string representation
    final_df['genres'] = final_df['genres'].apply(lambda x: ', '.join(x))

    # ticketRatio = audiCnt / showCnt, 상영 당 관객수
    final_df["ticketRatio"] = final_df["audiCnt"]/final_df["showCnt"]

    # final_df에는 openDt가 없기 때문에 summary에서 추가
    movie_summary = summary[["movieCd", "openDt"]]
    movie_summary['openDt'] = pd.to_datetime(summary['openDt'], format='%Y%m%d')

    pred_df = final_df[["date", "movieCd", "salesAmt", "genres", "ticketRatio"]]
    pred_df["date"] = pd.to_datetime(pred_df['date'], format='%Y%m%d')
    pred_set = pd.merge(pred_df, movie_summary.drop_duplicates(), on='movieCd', how='left')

    # 데이터 날짜 기준 상영일수
    pred_set["showingDays"] = pred_set["date"] - pred_set["openDt"]
    pred_set["showingDays"] = pred_set["showingDays"].dt.days
    pred_set = pred_set.dropna()

    pred_set.to_csv('pred_set.csv', index=False)

    print("saved pred_set.csv")

def run():
    get_pred_data()

def main():
    run()

if __name__ == "__main__":
    main()