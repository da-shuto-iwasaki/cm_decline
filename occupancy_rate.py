# coding: utf-8

"""
ファイルの概要：どこかの局でCMが流れる直前の、全てのテレビ局の視聴率データを取得する。（世帯のみ）
mom = int(sys.argv[1]) # 幾つで並列処理をするか
son = int(sys.argv[2]) # データの何番目か
を取得するため、
「nohup python occupancy_rate.py （幾つで並列処理を行うか）（何番目か） &」
という呼び方をする。
"""

import sys
import time
sys.path.append('../..') # daoを呼び出すため
from util import dao   # big_query からデータを持ってくる
import pandas as pd

def get_target_rate(df):
    """
    関数の概要：単純に視聴率データを拾ってくる。
    @param df         ：CMの情報が入っているデータ(before_bigqueryで整形済み)
    @return df：号数を計算し、四捨五入して整形したデータフレーム
    """
    time = str(df["before_cm_time"])[-4:]      # 指定した時間（下4桁）
    housouday = str(df["before_cm_time"])[:-4] # 日にち

    query="SELECT setai, media_id, time, housou_day "+ "FROM vr_minute WHERE time = "+ time + " AND housou_day = "+ housouday
    df = dao.read_sql_data(query, db_type='db_prod_kanto')
    # エラー処理。
    if len(df.values)==0: return ["-","-","-","-","-","-","-"]
    return df.values.T[0].tolist()

# メインの処理
if __name__ == "__main__":
    start = time.time() # プログラム開始時間
    year_month = sys.argv[1]
    # データの読み込み
    CMinfo = pd.read_csv('A_time/CM/prepro_' + year_month + '.csv')
    seven_col = ['media_code','housou_day', 'before_cm_time','broadcast_datetime',
                 'broadcast_end_datetime', 'after_cm_time', 'after_cm_5_time']
    CM_tmp = CMinfo.loc[:,seven_col] # 欲しいデータのタイプに合わせて7カラムにする。
    colnames = ['JOAB', 'JOAK', 'JOAX', 'JOCX', 'JOEX', 'JORX', 'JOTX']

    # それぞれ指定した時間で欲しい視聴率をとってくる。
    df_cm_rate = CM_tmp.apply(lambda x:get_target_rate(x), axis=1)
    df_cm_rate.columns = colnames                       # カラム名を整える
    df_cm_rate.to_csv('A_time/rate/' + year_month + '_occupancy.csv', index = False)
    print("{} [occupancy]開始から {:.2f}[s] の時間が経ちました。CMの数は{}です。".format(year_month, time.time()-start, len(CMinfo)))
