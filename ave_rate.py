# coding: utf-8
"""
ファイルの概要：各TVの欲しい階層の視聴率データを取得する。
setai,kozin,C,T,M1,M2,M3,F1,F2,F3の各層の視聴率データを取ってくる。
なお、取ってくる時間帯はCM終了
"""
import sys
sys.path.append('../..') # daoを呼び出すため
from util import dao   # big_query からデータを持ってくる
import pandas as pd
import time

def get_target_rate(df):
    """
    関数の概要：big query から目的のデータを取ってくる。
    　　　　　　この関数では、テレビの視聴率データを平均することで持ってくる。
    """
    start_day  = str(df["housou_day_s"])
    end_day    = str(df["housou_day_e"])
    start_time = str(df["time_s"])
    end_time   = str(df["tim_(e"])
    media_id   = df["media_id"]
    query = "SELECT AVG(setai),AVG(kozin),AVG(child0412),AVG(child1319),AVG(M1),AVG(M2),AVG(M3),AVG(F1),AVG(F2),AVG(F3) "+ "FROM vr_minute WHERE media_id='" + media_id

    # 日にちをまたいでいるかどうかでこの後が変わる。
    if start_day != end_day:
        query += "' AND (housou_day = '" + start_day + "' AND time >= "+start_time + ") OR (housou_day = '" + end_day + "' AND time<= " + end_time + ")"
    else:
        query += "' AND housou_day = '" + start_day + "' AND time>="+start_time+" AND time<="+end_time
    df = dao.read_sql_data(query, db_type='db_prod_kanto')
    # エラー処理
    if len(df.values)==0: return ["-","-","-","-","-","-","-","-","-","-"]
    return df.values[0].tolist()

# メインの処理
if __name__ == "__main__":
    start = time.time() # プログラム開始時間
    year_month = sys.argv[1]
    # データの読み込み
    VRinfo = pd.read_csv('A_time/CM/prepro_' + year_month + '.csv')
    ten_col = ['media_id', 'housou_day_s', 'housou_day_e', 'time_s','time_e',
               'id','ken_id','keishiki', 'nettype', 'shubetu']
    VR_tmp = VRinfo.loc[:,ten_col] # 欲しいデータのタイプに合わせて10カラムにする。
    colname = ["setai","kozin","C","T","M1","M2","M3","F1","F2","F3"]

    # 指定した時間帯の欲しい視聴率をとってくる。
    df_VR_rate = VR_tmp.apply(lambda x:get_target_rate(x), axis=1)
    colname = [colname[i]+"_rate" for i in range(len(colname))]
    df_VR_rate.columns = colname
    df_VR_rate.to_csv('A_time/rate/' + year_month + '_ave_vr_rate.csv', index = False)
    print("{} [ave VR]開始から {:.2f}[s] の時間が経ちました。CMの数は{}です。".format(year_month, time.time()-start, len(VRinfo)))
