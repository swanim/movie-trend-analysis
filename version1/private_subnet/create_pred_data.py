import pandas as pd
import numpy as np
from scipy import stats


def drop_duplicates_in_list(lst):
    return list(set(lst))

def get_pred_data():
    sales = pd.read_csv('/home/ec2-user/project/raw_data/sale/movie_sale.csv')
    summary = pd.read_csv('/home/ec2-user/project/raw_data/summary/movie_summary.csv')
    genre = pd.read_csv('/home/ec2-user/project/raw_data/genre/movie_genre.csv')

    sales_df = sales[['date','movieCd', 'movieNm', 'salesAmt', 'salesShare', 'scrnCnt', 'showCnt', 'audiCnt']]
    sales_df2 = sales_df[(np.abs(stats.zscore(sales_df[['audiCnt', 'salesAmt', 'movieCd']])) < 2).all(axis=1)]

    merged_df = pd.merge(sales_df2, genre, on='movieCd', how='inner')

    grouped_df = merged_df.groupby('movieCd')['genres'].apply(list).reset_index(name='genres')
    grouped_df['genres'] = grouped_df['genres'].apply(drop_duplicates_in_list)

    final_df = pd.merge(sales_df2, grouped_df, on='movieCd', how='left')

    final_df['genres'] = final_df['genres'].apply(lambda x: ', '.join(x))

    final_df["ticketRatio"] = final_df["audiCnt"]/final_df["showCnt"]
    movie_summary = summary[["movieCd", "openDt"]]
    movie_summary['openDt'] = pd.to_datetime(summary['openDt'], format='%Y%m%d')

    pred_df = final_df[["date", "movieCd", "salesAmt", "genres", "ticketRatio"]]
    pred_df["date"] = pd.to_datetime(pred_df['date'], format='%Y%m%d')

    pred_set = pd.merge(pred_df, movie_summary.drop_duplicates(), on='movieCd', how='left')

    pred_set["showingDays"] = pred_set["date"] - pred_set["openDt"]
    pred_set["showingDays"] = pred_set["showingDays"].dt.days
    pred_set = pred_set.dropna()

    pred_set.to_csv('/home/ec2-user/project/pred_set.csv', index=False)

    print("saved pred_set.csv")

def run():
    get_pred_data()

def main():
    run()

if __name__ == "__main__":
    main()