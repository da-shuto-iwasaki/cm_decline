# coding: utf-8

"""
ファイルの概要：CM のデータに TV のデータを結びつける。
CMinfo:CMのデータ
VRinfo:TVのデータ
"""
import datetime
import pandas as pd
import time

# この関数は、返すデータ(TVデータ)と元となるデータ(CMデータ)のカラム数が一致しないとうまくいかない。
def make_vr_info_fix_row(in_row, vr_info_fix_df):
    """
    関数の概要：
    @param in_row        ：データフレーム（CM情報）の行。
    @param vr_info_fix_df：上で整形した vr_info（番組情報）
    @return              ：各CMが流れた番組の情報のリスト形式。
    """     
    dt = in_row.broadcast_datetime # CM放送日
    media_code = in_row.media_code # CMが流された局の名前

    start_datetime = pd.to_datetime(vr_info_fix_df.start_datetime, format='%Y-%m-%dT%H:%M:%S') # 番組の開始時間
    end_datetime = pd.to_datetime(vr_info_fix_df.end_datetime, format='%Y-%m-%dT%H:%M:%S') # 番組の終了時間
    
    # CMが流れた時の番組（局が同じで）のデータ
    row = vr_info_fix_df[(vr_info_fix_df.media_id == media_code) & # 局が同じ
                         (start_datetime <= dt) & #  テレビ番組放送開始時間 <= CM放送時間
                         (dt <= end_datetime)]    # CM放送日 < = テレビ番組放送終了時間

    return row.values[0].tolist()

# メインの処理。
if __name__ == '__main__':
    # 処理開始時間
    start = time.time()
    # データの読み込み
    CMinfo = pd.read_csv('CMinfo/201707_VR_preprocessing.csv', index_col=0)
    VRinfo = pd.read_csv('VRinfo/201707_VR_preprocessing.csv', index_col=0)

    # 追加しないといけないカラム数
    N_add_col = len(VRinfo.columns) - len(CMinfo.columns)
    df_tmp = CMinfo[:] # 参照の違う同じデータフレーム を作成する。

    # 関数処理のために、列数を一致させる必要がある。
    for i in range(N_add_col):
        df_tmp[str(i)] = 0

    # 目的のデータを選び出し、CMの情報に加える。
    added_df = df_tmp.apply(lambda x: make_vr_info_fix_row(x, VRinfo), axis=1)
    # カラムの名前を合わせる。
    added_df.columns = VRinfo.columns
    # 横に結合する。（番組情報を横に結合する。）
    df = pd.concat([CMinfo, added_df], axis=1)
    
    df.to_csv('CMinfo/201707_VR_preprocessing.csv') # ここまででセーブポイントを用意しておく。
    
    # 不要なカラムを除く。
    df = df.drop('flag', axis=1)

    df.to_csv('CMinfo/201707_VR_preprocessing.csv')
    print("CMデータ数:{}, TVデータ数:{}で {}[s] の時間がかかりました。".format(len(CMinfo), len(VRinfo), time.time()-start))