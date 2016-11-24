#!/usr/bin/python
# coding: UTF-8

'''
周期判定

python detect_period.py xxxx.dat
'''

import collections
import pprint
import re
import sys
import numpy as np
import sqlite3
import matplotlib.pyplot as plt
import math
import time
import burst_detect

DATE_OFFSET = 1
TIME_OFFSET = 2

filename = sys.argv[1]
fd = open(filename,"r")

'''
Time Stampの取得(秒換算)
'''

time_lists = {}
group_id = 0
for i in fd.readlines():#1行ずつ読み込む
    line = i.split()
    if line[0] == 'group':#group が来るたびにグループ分け
        group_id = int(line[1])
        time_lists[group_id] = []
    else:
        time_stamp = line[TIME_OFFSET].split(':')
        time_sec = int(time_stamp[0])*60*60+int(time_stamp[1])*60+int(time_stamp[2])
        time_lists[group_id].append(time_sec)
fd.close()


for ind,time_list in time_lists.items():
    # if ind > 3:
    #     continue
    #0-86400secの時系列データ化
    time_series = np.zeros(24*60*60)
    for j in time_list:
        # time_series[j] += 1
        time_series[j] = 1
        # break
    cor = np.correlate(time_series,time_series[:86400 - 3600])#全区間で自己共分散計算
    print(ind)
    print(sorted({k:v for k,v in enumerate(cor[1:3601],start = 1)}.items(),key = lambda x:x[1])[:-11:-1])


    #0secずらしを除いて、一番最初のピークを求める。
    maxcor = 0
    maxind = 0
    for i,row in enumerate(cor[1:3601],start = 1):
        if row > maxcor:
            maxcor = row
            maxind = i
    print('result',ind,':','max cor =',maxcor,'period =',maxind,'data cnt=',len(time_list))
    for offset in range(1,6):
        if offset * maxind < 3601:
            print(maxind * offset,cor[maxind*offset],end=',')
    print('\n')
    del(cor)
