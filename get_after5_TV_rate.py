# coding: utf-8

"""
ファイルの概要：TV番組開始5分の視聴率データを取ってくる。
setai,kozin,C,T,M1,M2,M3,F1,F2,F3の各層の視聴率データを取ってくる。
"""

import sys
sys.path.append('../..') # daoを呼び出すため
from util import dao   # big_query からデータを持ってくる
import pandas as pd
import time
from datetime import datetime

def get_after_5(df):
    start_day  = int(df["housou_day(s)"]) # int型
    start_time = int(df["time(s)"]) # int型
    end_time   = int(df["time(e)"]) # int型
    media_id   = df["media_id"]
    start_time += 5 # 5分後を考える。日にちをまたいでしまった時の処理が以下
    if start_time>2859:
        start_time-=2400
        start_day+=1
    if start_time<end_time:
        query = "SELECT setai,kozin,child0412,child1319,M1,M2,M3,F1,F2,F3 FROM vr_minute WHERE media_id='"+media_id+"' AND housou_day = '"+str(start_day)+"' AND time="+str(start_time)
        df = dao.read_sql_data(query, db_type='db_prod_kanto')
        # エラー処理
        if len(df.values)==0: return ["-","-","-","-","-","-","-","-","-","-"]
        else: return df.values[0].tolist()
    else:
        return [False,"-","-","-","-","-","-","-","-","-"]
    
# メインの処理
if __name__ == "__main__":
    # データの読み込み
    VRinfo = pd.read_csv('VRinfo/201707_VR_preprocessing_for_query.csv', index_col=0).reset_index(drop=True)
    ten_col = ['media_id', 'housou_day(s)', 'housou_day(e)', 'time(s)','time(e)',
               'id','ken_id','keishiki', 'nettype', 'shubetu']
    VR_tmp = VRinfo.loc[:,ten_col] # 欲しいデータのタイプに合わせて10カラムにする。    
    colname = ["setai","kozin","C","T","M1","M2","M3","F1","F2","F3"]
    start = time.time() # プログラム開始時間
    print("今の時刻は(9時間後を考える){}".format(datetime.now()))
    
    # それぞれ指定した時間で欲しい視聴率をとってくる。
    df_VR_rate = VR_tmp.apply(lambda x:get_after_5(x), axis=1)
    colname = [colname[i]+"_after_5_rate" for i in range(len(colname))]
    df_VR_rate.columns = colname
    df_VR_rate.to_csv("VRinfo/201707_VR_after_5_rate.csv")
    df_VR_rate = pd.concat([VRinfo,df_VR_rate], axis=1) # 元のデータに結合する。
    print("[rate]開始から {}[s] の時間が経ちました。TV番組の数は{}です。".format(time.time()-start, len(VRinfo)))
