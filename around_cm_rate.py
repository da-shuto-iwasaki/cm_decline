# coding: utf-8
"""
ファイルの概要：各CMが終了してから1分後、5分後のデータを取得する。
setai,kozin,C,T,M1,M2,M3,F1,F2,F3の各層の視聴率データを取ってくる。
"""
import sys
import time
sys.path.append('../..') # daoを呼び出すため
from util import dao   # big_query からデータを持ってくる
import pandas as pd

def get_target_rate(df, target_time):
    """
    関数の概要：単純に視聴率データを拾ってくる。
    @param df         ：CMの情報が入っているデータ(before_bigqueryで整形済み)
    @param target_time：欲しい時間（カラム名で指定）
    @return df：号数を計算し、四捨五入して整形したデータフレーム
    """
    housoukyoku = df.media_code          # 放送局
    time = str(df[target_time])[-4:]      # 指定した時間（下4桁）
    housouday = str(df[target_time])[:-4] # 日にち

    query="SELECT setai,kozin,child0412,child1319,M1,M2,M3,F1,F2,F3 "+ "FROM vr_minute WHERE time = "+ time + " AND media_id = '" +housoukyoku+ "' AND housou_day = "+ housouday
    df = dao.read_sql_data(query, db_type='db_prod_kanto')
    # エラー処理。
    if len(df.values)==0: return ["-","-","-","-","-","-","-","-","-","-"]
    return df.values[0].tolist()

# メインの処理
if __name__ == "__main__":
    start = time.time() # プログラム開始時間
    year_month = sys.argv[1]
    # データの読み込み
    CMinfo = pd.read_csv('A_time/CM/prepro_' + year_month + '.csv')
    ten_col = ['media_code', 'housou_day', 'onair_sec', 'CM_genre','CM_type', 'before_cm_time',
               'broadcast_datetime','broadcast_end_datetime', 'after_cm_time', 'after_cm_5_time']
    CM_tmp = CMinfo.loc[:,ten_col] # 欲しいデータのタイプに合わせて10カラムにする。
    colname = ["setai","kozin","C","T","M1","M2","M3","F1","F2","F3"]

    # それぞれ指定した時間で欲しい視聴率をとってくる。
    """ここから big query に投げてデータの取得"""
    df_after_cm_time = CM_tmp.apply(lambda x:get_target_rate(x, 'after_cm_time'), axis=1)
    after_col_name = ["after_"+colname[i] for i in range(len(colname))]
    df_after_cm_time.columns = after_col_name
    df_after_cm_time.to_csv('A_time/rate/' + year_month + '_after_cm_time.csv', index = False)
    print("{} [after]開始から {:.2f}[s] の時間が経ちました。CMの数は{}です。".format(year_month, time.time()-start, len(CMinfo)))


    df_after_cm_5_time = CM_tmp.apply(lambda x:get_target_rate(x, 'after_cm_5_time'), axis=1)
    after_5_col_name = ["after_5_"+colname[i] for i in range(len(colname))]
    df_after_cm_5_time.columns = after_5_col_name
    df_after_cm_5_time.to_csv('A_time/rate/' + year_month + '_after_cm_5_time.csv', index = False)
    print("{} [after 5]開始から {:.2f}[s] の時間が経ちました。CMの数は{}です。".format(year_month, time.time()-start, len(CMinfo)))


    df_before_cm_time = CM_tmp.apply(lambda x:get_target_rate(x, 'before_cm_time'), axis=1)
    before_col_name = ["before_"+colname[i] for i in range(len(colname))]
    df_before_cm_time.columns = before_col_name
    df_before_cm_time.to_csv('A_time/rate/' + year_month + '_before_cm_time.csv', index = False)
    print("{} [before]開始から {:.2f}[s] の時間が経ちました。CMの数は{}です。".format(year_month, time.time()-start, len(CMinfo)))
