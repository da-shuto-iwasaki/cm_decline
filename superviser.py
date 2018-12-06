# coding: utf-8

"""
ファイルの概要：2016/01〜2017/08 まであるが、CPUが4つなので、
                ４つ１組で並列処理を行なっていく。
"""

import subprocess
# 使えるデータは、2016/01 〜 2017/08 であることに注意！！
months = [201601 + i for i in range(12)] + [201701 + i for i in range(8)]
count = 0

# 5回の処理に分けて実行する。
for i in range(5):
    procs=[]
    for j in range(4):
        # nohup python occupancy_rate.py [201601] & という形で動かしたい。
        proc = subprocess.Popen(['nohup','python','occupancy_rate.py','{}'.format(months[count]),'&'])
        procs.append(proc)
        count += 1

    for proc in procs:
        proc.communicate()