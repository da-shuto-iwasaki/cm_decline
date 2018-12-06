"""
ファイルの概要：Aタイムに絞って視聴率データを取得しつつ、必要なカラムを作成する。
Aタイム：平日のプライムタイム（19時～23時）の４時間と土日の18時～23時の５時間。最も視聴率が高く、それと同時に値段も高い。
time = sys.argv[1] # いつのデータを利用するか 例：201707 を取得するため、
「nohup python preprocessing.py (年月) &」という呼び方をする。

○内容
・同じ番組内に流れているCMをまとめる。
・番組情報を付加する。
・カラムの整頓。
○データの流れ
`./VRtarget/[time]_VR.xlsx` + `./A_time/TV/vr_info.fix_[time].csv` → `./A_time/CM/prepro_[time].csv`
"""
import sys
import pandas as pd
import subprocess
import datetime
import time

def convert_date_to_intform(value):
    """
    関数の概要：Timestanp型のデータを整数(bigqueryに合った形)にする。
    @param value: 時間のデータ。Timestamp型。
    @return     : 結合された時間のデータ。
    """
    time_data = 0
    time_data += value.day
    time_data += value.month * 100
    time_data += value.year  * 10000
    return time_data

def make_broadcast_start_datetime(row):
    """
    関数の概要：日付の修正をし、時間と分数のデータも追加する。
    @param row：データフレームの各行。
    @return   ：CM開始時間。
    """
    dt = row.broadcast_date # 放送日のみのデータ
    if row.is_tomorrow == True: # 翌日だったら
        dt = dt + datetime.timedelta(days=1) # 日付を遅らせる。
    # 時間、分数のデータを追加
    dt = dt + datetime.timedelta(hours=row.hour)
    dt = dt + datetime.timedelta(minutes=row.minutes)
    return dt

def make_broadcast_end_datetime(row):
    """
    関数の概要：放送時間を追加し、CM終了時間を求める。
    @param row：データフレームの各行。
    @return   ：CM終了時間。
    """
    dt = row.broadcast_datetime # 放送開始時間
    return dt + datetime.timedelta(seconds=row.onair_sec)

def make_before_cm_time(broadcast_datetime):
    """
    関数の概要：CM放送時間の1分前を求める。（そこを基準値として視聴率を見るため）
    @param broadcast_datetime：時間(Timestamp('2017-07-01 05:00:00')的なの)
    @return                  ：CM放送1分前
    """
    return broadcast_datetime + datetime.timedelta(minutes=-1)

def make_after_cm_time(broadcast_end_datetime):
    """
    関数の概要：CM放送終了時刻（分）を求める。終了後30秒残ってるかどうかがポイント。
    @param broadcast_end_datetime：時間(Timestamp('2017-07-01 05:00:30')的なの)
    @return                      ：CM放送1分前
    """
    if broadcast_end_datetime.second < 30:
        return broadcast_end_datetime
    else:
        return broadcast_end_datetime + datetime.timedelta(minutes=1)

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

    # Aタイムじゃなかったら見つからない場合は、何もないカラムを入れる。
    if len(row) == 0 : return ["-" for i in range(25)]
    return row.values[0].tolist()

def arrange_time_data(timedata, housou_day):
    """
    関数の概要：big query の housou_day の形式に合わせる。(housou_day=True)
    　　　　　　または、time の形式に合わせる。（housou_day=False）
    """
    # timestanpのデータを取ってきて、整形する。
    time   = datetime.datetime.strptime(timedata, '%Y-%m-%d %H:%M:%S')
    year   = time.year
    month  = time.month
    day    = time.day
    hour   = time.hour
    minute = time.minute
    if hour<5: # もしも5時より前だったら、24時間足して1日前の日付を利用する。
        hour += 24
        day -= 1
        if day == 0:
            day =31
            month -=1
    if housou_day: return year*10000+month*100+day
    else: return hour*100+minute

def is_sb(row):
    """
    関数の概要：CMが流れたタイミングによって異なるラベルをつける
    @param row：データフレームの行データ（applyで各行に処理を行う。）
    @return   ：ラベル
    """
    if row.start_time_work == row.program_start_time_work:     # CM開始時間が番組開始時間と等しい場合
        return 1
    elif row.end_time_work == row.program_end_time_work:       # CM終了時間が番組終了時間と等しい場合
        return 2
    elif row.end_time_work_plus1 == row.program_end_time_work: # CM終了時刻1分後と番組終了が一致した場合（これもステブレに含める(ニアHH)）
        return 3
    elif row.end_time_work > row.program_end_time_work:        # CMが番組をまたがった場合
        return 4
    else:
        return 0

if __name__=="__main__":
    start_time = time.time() # プログラム開始時間

    """ 【CM情報の整形とカラムの作成】 """

    year_month = sys.argv[1]
    CMinfo = pd.read_excel('VRtarget/'+ year_month + '_VR.xlsx')
    media_code_map = dict(CXT="JOCX",EXT="JOEX",NTV="JOAX",TBS="JORX",TXT="JOTX")
    CMinfo['media_code'] = CMinfo['局'].apply(lambda x: media_code_map[x]) # 曲のデータを書き換える。
    CMinfo['onair_sec'] = CMinfo['秒数']        # CMが流れた時間。
    CMinfo['broadcast_date'] = CMinfo['出稿日'] # CMが流れた日にち。
    CMinfo['housou_day'] = CMinfo.broadcast_date.apply(lambda x: convert_date_to_intform(x))
    CMinfo['hour'] = CMinfo['時間(hh:mm)'].apply(lambda x: int(x[0:2]))
    CMinfo['minutes'] = CMinfo['時間(hh:mm)'].apply(lambda x: int(x[3:6]))
    # 翌日のデータ華道家のフラグを立てることで、時間を0~24時の範囲に収める
    CMinfo['is_tomorrow'] = CMinfo['hour'].apply(lambda x: True if (x >= 24) else False)
    CMinfo['hour'] = CMinfo['hour'].apply(lambda x: x - 24 if (x >= 24) else x)
    CMinfo['broadcast_datetime'] = CMinfo.apply(lambda x: make_broadcast_start_datetime(x) , axis=1)
    CMinfo['broadcast_end_datetime'] = CMinfo.apply(lambda x: make_broadcast_end_datetime(x) , axis=1)
    CMinfo['before_cm_time'] = CMinfo.broadcast_datetime.apply(lambda x: make_before_cm_time(x))
    CMinfo['before_cm_time'] = CMinfo.before_cm_time.apply(lambda x: convert_date_to_intform(x))
    CMinfo['after_cm_time'] = CMinfo.broadcast_end_datetime.apply(lambda x: make_after_cm_time(x))
    CMinfo['after_cm_time'] = CMinfo.after_cm_time.apply(lambda x: convert_date_to_intform(x))
    CMinfo['after_cm_5_time'] = CMinfo['after_cm_time'].apply(lambda x:x + 4)
    # カラム名を整理する。
    CMinfo['bangumi_name'] = CMinfo['番組名']
    CMinfo['CM_type'] = CMinfo['CM区分(番組/スポット/サス)']
    CMinfo['CM_genre'] = CMinfo['業種']
    # 残したいカラムだけにする。
    CMpickup_col = ['media_code','bangumi_name','housou_day','onair_sec','CM_genre','CM_type','before_cm_time','broadcast_datetime','broadcast_end_datetime','after_cm_time','after_cm_5_time']
    CMinfo = CMinfo[CMpickup_col].reset_index(drop=True)

    """  【番組情報の整形】  """

    VRinfo = pd.read_csv('A_time/TV/vr_info_fix_'+ year_month + '.csv', index_col=0)
    VRpickup_col = list(VRinfo.columns)
    VRinfo = VRinfo.sort_values(by=['media_id', 'start_datetime'], ascending=True).reset_index(drop=True) # 以下の処理を行うため、並び替える。
    while True:
        """
        同じ番組なのに、一部・二部のように分かれているものがある。
        そのため、それを一つにまとめる。（上のstart_datetimeを引き継ぎ、下のend_datetimeを受け取る。）
        部構成がいくつ繋がっているかわからないため、whileで無くなるまで回し続ける。
        """
        VRinfo['flag'] = False # 一度全てFalseにする。（初期化。）
        same_name_after = (VRinfo['bangumi_name'] == VRinfo['bangumi_name'].shift(-1))     # 一つ下のデータと番組の名前が「等しい」かどうか。
        different_name_before = (VRinfo['bangumi_name'] != VRinfo['bangumi_name'].shift(1)) # 一つ上のデータと番組の名前が「等しくない」かどうか。
        renzoku_after   = (VRinfo['end_datetime'] == VRinfo['start_datetime'].shift(-1))   # 一つ前の番組から続いているかどうか。
        # フラグがTrue ＝ 「連続した部構成の『一番上』の行」
        VRinfo['flag'] = same_name_after & renzoku_after & different_name_before

        if (len(VRinfo[VRinfo['flag'] == True]) == 0): # フラグがTrueのものがなくなったら終わり。
            break
        # 開始時間は、上がTrueだったらそれを引き継ぎ、Falseだったら今持っているものがそのまま。（変わらない。）
        VRinfo['start_datetime'] = VRinfo['start_datetime'].shift(1).where(VRinfo['flag'].shift(1) == True, VRinfo['start_datetime'] )
        VRinfo = VRinfo[VRinfo['flag'] == False] # Trueのものはもういらないので削除する。

    N_add_col = len(VRinfo.columns) - len(CMinfo.columns)
    df_tmp = CMinfo[:]
    for i in range(N_add_col): # カラム数を合わせるために追加しているだけ。特に意味はない。
        df_tmp[str(i)] = i

    """ 〜【CMに番組情報を付加】〜 """

    added_df = df_tmp.apply(lambda x: make_vr_info_fix_row(x, VRinfo), axis=1) # 目的のデータを選び出し、CMの情報に加える。
    added_df[added_df.media_code != "-"].reset_index(drop=True)
    added_df.columns = VRinfo.columns # カラムの名前を合わせる。
    added_df = added_df.drop(["bangumi_name","housou_day"], axis=1) # 被るカラムを除いておく。
    df = pd.concat([CMinfo, added_df], axis=1)
    # Aタイム時間を除く。
    df = df[df.id != "-"].reset_index(drop=True)
    df = df.drop("flag", axis=1)
    #=== big query 用にデータの形式を整える ===
    df['housou_day_s'] = df.start_datetime.apply(lambda x:arrange_time_data(x, True))
    df['housou_day_e'] = df.end_datetime.apply(lambda x:arrange_time_data(x, True))
    df['time_s'] = df.start_datetime.apply(lambda x:arrange_time_data(x, False))
    df['time_e'] = df.end_datetime.apply(lambda x:arrange_time_data(x, False))

    """ 【CM群の作成（まとめる）】 """

    df['start_time_work']         = df.broadcast_datetime.apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    df['end_time_work']           = df.broadcast_end_datetime.apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    df['end_time_work']           = df.end_time_work.apply(lambda x: datetime.datetime.strptime(x.strftime('%Y-%m-%d %H:%M'), '%Y-%m-%d %H:%M'))
    df = df.sort_values(by=['media_code', 'broadcast_datetime'], ascending=True)
    df['end_time_work_plus1']     = df.end_time_work.apply(lambda x: x + datetime.timedelta(minutes=1))
    df['program_start_time_work'] = df.start_datetime.apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    df['program_end_time_work']   = df.end_datetime.apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    df['total_onair_sec']         = df['onair_sec']
    df['cm_count'] = 1

    while True:
        """
        同じ枠内で放送されていたCMを全て一つにまとめる。
        なお、番組をまたいだ場合、たとえCM枠が続いても番組をまたいだタイミングで二つに分ける。
        同じ枠にいくつCMが存在しているかわからないため、whileで無くなるまで回し続ける。
        """
        same_name_before     = (df['bangumi_name']    == df['bangumi_name'].shift(1)) # 一つ「前」のCMが流れていた番組と「同じ」番組か
        different_name_after = (df['bangumi_name']    != df['bangumi_name'].shift(-1)) # 一つ「後」のCMが流れていた番組と「違う」番組か
        renzoku_before       = (df['start_time_work'] == df['end_time_work'].shift(1))   |  (df['start_time_work']    == df['end_time_work_plus1'].shift(1)) # 前のCMと同じ枠だったか
        not_renzoku_after    = (df['end_time_work']   != df['start_time_work'].shift(-1)) & (df['end_time_work_plus1'] != df['start_time_work'].shift(-1)) # その枠がそのCMで終わっていたか
        # 一つ前のCMが流れたいた番組と同じ番組名であり、前のCMと同じ枠で、「そのCMがその枠最後のCMであった」or「その番組で最後のCMであった」ならTrue
        df['flag']       = (same_name_before & renzoku_before & not_renzoku_after) | (same_name_before & renzoku_before & different_name_after)

        if (len(df[df['flag'] == True]) == 0):# Trueのものがなくなったら終わり。
            break

        # 上でフラグがTrueのもの(最後の枠)は、一つ上の枠のCM枠に時間を足す。（これを繰り返せば、合計時間が計算できる。）また、CM数も記録する。
        df['total_onair_sec'] = df['total_onair_sec'] + df['total_onair_sec'].shift(-1).where(df['flag'].shift(-1) == True, 0)
        df['cm_count']        = df['cm_count']        + df['cm_count'].shift(-1).where(df['flag'].shift(-1) == True, 0)

        """ 内容を引き継ぐカラム（終了時刻を示す。）"""
        # 終了タイミングのデータ
        bed = 'broadcast_end_datetime'
        etw = 'end_time_work'
        etwp1 = 'end_time_work_plus1'
        acm = 'after_cm_time'
        acm5 = 'after_cm_5_time'

        df[bed]   = df[bed].shift(-1).where(df['flag'].shift(-1) == True, df[bed])
        df[etw]   = df[etw].shift(-1).where(df['flag'].shift(-1) == True, df[etw])
        df[etwp1] = df[etwp1].shift(-1).where(df['flag'].shift(-1) == True, df[etwp1])
        df[acm]   = df[acm].shift(-1).where(df['flag'].shift(-1) == True, df[acm])
        df[acm5]  = df[acm5].shift(-1).where(df['flag'].shift(-1) == True, df[acm5])

        df = df[df['flag'] == False]

    # カラムを追加する。
    df['is_sb']           = df.apply(lambda x: is_sb(x), axis=1)
    df['after_cm_time']   = df['after_cm_time'].astype(int)
    df['after_cm_5_time'] = df['after_cm_5_time'].astype(int)
    df = df.drop(["flag"], axis=1).reset_index(drop=True)
    # データの出力
    df.to_csv('A_time/CM/prepro_' + year_month + '.csv', index=False)

    """ 【視聴率データをbigqueryから取り出す作業（並列で回す。）】 """

    programs = ["ave_rate.py","occupancy_rate.py","around_cm_rate.py"]
    procs=[]
    for i in range(len(programs)):
        proc = subprocess.Popen(['nohup','python','{}'.format(programs[i]),'{}'.format(year_month),'&'])
        procs.append(proc)

    for proc in procs:
        proc.communicate()

    """ 【終了】 """

    end_time = time.time() # プログラム開始時間
    print("{}のデータ整形が終了しました。経過時間は{:.2f}[s]です。".format(end_time - start_time))
