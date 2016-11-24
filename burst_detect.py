#!/usr/bin/python
# coding: UTF-8

'''
.datファイルを読み込んで、テンプレidごとにバースト計算する。

python busrt_detect.py xxx.dat offset
'''

import collections
import pprint
import re
import sys
import numpy as np
import sqlite3
import datetime
import matplotlib.pyplot as plt
import pybursts
import math
import datetime

TIME_OFFSET = int(sys.argv[2])

FILENAME = sys.argv[1]
FD = open(FILENAME,"r")


'''
Time Stampの取得(秒換算)
'''
def get_time_stamp():
    time_lists = {}
    group_id = 0
    for i in FD.readlines():#1行ずつ読み込む
        line = i.split()
        if line[0] == 'group':#group が来るたびにグループ分け
            group_id = int(line[1])
            time_lists[group_id] = []
        else:
            time_stamp = line[TIME_OFFSET].split(':')
            time_sec = int(time_stamp[0])*60*60+int(time_stamp[1])*60+int(time_stamp[2])
            # print(time_lists[group_id])
            if time_lists[group_id] != [] and math.floor(time_lists[group_id][-1]) == time_sec:#秒単位でも重複がある場合は少数3桁でカウントしていく
                time_sec = round(time_lists[group_id][-1] + 0.001,3)
            time_lists[group_id].append(time_sec)
    # time_lists[group_id].append(86400)
    FD.close()
    return time_lists


'''
レベルの重複削除
Before
[[0 7079 65511]
 [1.0 54134 55689]
 [2.0 54134 55689]
 [3.0 55655 55689]
 [4.0 55655 55689]
 [5.0 55655 55689]
 [6.0 55655 55689]
 [7.0 55655 55689]
 [8.0 55655 55689]
 [9.0 55655 55689]
 [10.0 55655 55689]]

After
[[2.0 54134 55689]
 [10.0 55655 55689]]
'''
def burst_detect(time_lists):
    burst_result = []
    for k,v in time_lists.items():
        time_list = list(v)#参照渡しではなくコピー
        if len(time_list) > 30:#量でフィルタ
            #最初と最後が0と86400じゃなかったら臨時で追加
            if time_list[-1] < 86400:
                time_list.append(86400)
            if time_list[0] != 0:
                time_list.insert(0,0)
            #バースト検知
            burst_list = pybursts.pybursts.kleinberg(sorted(set(time_list)),s=2,gamma=1.0)

            #ここで重複レベルを削除
            for j in range(len(burst_list)-1):
                if not any([x-y for x,y in zip(burst_list[j][1:],burst_list[j+1][1:])]):
                    burst_list[j] = [0,0,0]
            burst_list = np.delete(burst_list,np.where(burst_list == 0)[0],0)

            #ここでintervalが1min超える場合は削除
            #burst_list = check_interval(burst_list,i[1:])

            #暫定listが残っていたらresultに追加
            if len(burst_list) != 0:
                burst_result.append( [ k, burst_list ])
    return burst_result

#1groupのtime listを受ける。
def check_interval(burst_range,group_time_list):
    if burst_range == []:
        return burst_range
    burst_range_result = []
    sub_list = []
    # print('check interval',burst_range)

    for lv,s,e in burst_range:
        sub_list = [y-x for x,y in zip(group_time_list[:-1],group_time_list[1:]) if s <= x <= e and s <= y <= e ]
        sub_list_count = collections.Counter(sub_list)
        over_1min_interval_rate = sum([x for k,x in sub_list_count.items() if k > 60])/len(sub_list)
        if over_1min_interval_rate < 0.5:
            burst_range_result.append([lv,s,e])
        sub_list = []

    return burst_range_result

if __name__ == '__main__':

    time_lists = get_time_stamp()
    bursts = burst_detect(time_lists)

    bursts_dict = {k:v for k,v in bursts}
    for k,time_list in time_lists.items():
        print('timelist\n',time_list[0],time_list[-1],len(time_list))
    print('bursts_dict\n',bursts_dict)


    for i,time_list in time_lists.items():
        if not isinstance(bursts_dict.get(i,0),int):
            plt.figure(figsize=(25, 12))
            x = np.array(time_list)
            # y = [ z for z in range(len(x))]
            # plt.plot(x,y,label='incident')
            non_burst_time = time_list
            for lv,s,e in bursts_dict[i]:
                non_burst_time = [z for z in non_burst_time if s > z or e < z]
                # plt.hlines(y[-1]/2,s,e,color='red',linewidth='4')
            time_lists[i] = non_burst_time
            y = [ z for z in range(len(non_burst_time))]
            plt.plot(non_burst_time,y,label='log')
            plt.legend()
            plt.savefig("burst_ditect_after{0}.png".format(i))
    # print('non_burst_times',time_lists)
