# coding: utf-8

"""
ファイルの概要：vr_info_fix から、番組データを１ヶ月毎取得する。
　　　　　　　　ただし、番組を Aタイム に絞っている。なお、休日判断が大変だったため、平日も 18〜23 の間の番組のみを取得している。
"""
import sys
sys.path.append('../..') # daoを呼び出すため
from util import dao   # big_query からデータを持ってくる
import pandas as pd

if __name__ == "__main__":
    start_date = int(sys.argv[1]) # 開始月の初日
    end_date = start_date + 100 # 翌月
    remain_columns = ['id','ken_id','media_id','start_time','end_time','bangumi_name','sub_title','dai_genre_name',
                      'sho_genre_name','housou_day','week_name','housou_minutes','housou_kaisuu','housou_net_type',
                      'housou_keishiki','bangumi_shubetu','last_flag','sai_housou_flag','start_datetime','end_datetime',
                      'nettype','keishiki','shubetu','week52']
    query = "SELECT * \
            FROM vr_info_fix \
            WHERE housou_day >= "+ str(start_date) +" AND housou_day < " + str(end_date) +\
            " AND ( ( jikantai < 18 AND 23 <= end_jikantai ) OR (18 <= jikantai AND jikantai < 23) OR (18 <= end_jikantai AND end_jikantai < 23) )"

    df = dao.read_sql_data(query, db_type='db_prod_kanto')
    df = df[remain_columns]
    file_name = "A_time/TV/vr_info_fix_"+str(int((start_date-1)//100))+".csv"
    df.to_csv(file_name)
