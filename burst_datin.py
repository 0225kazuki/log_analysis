#!/usr/bin/python
# coding: UTF-8

'''
.datファイルを読み込んで、テンプレidごとにバースト計算する。
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

DATE_OFFSET = 2
TIME_OFFSET = 3

filename = sys.argv[1]
fd = open(filename,"r")

time_list = []
group_id = 0
for i in fd.readlines():
    if i.split()[0] == 'group':
        group_id = int(i.split()[1])
        time_list.append([group_id])
    else:
        time_stamp = i.split()[TIME_OFFSET].split(':')
        time_list[group_id - 1].append(int(time_stamp[0])*60*60+int(time_stamp[1])*60+int(time_stamp[2]))
fd.close()

burst_result = []
for i in time_list:
    if len(i) > 1:
        burst_result.append( [ i[0], pybursts.pybursts.kleinberg(sorted(set(i[1:])),s=2,gamma=1.0) ])

for i in time_list:
    plt.figure(figsize=(25, 12))
    i = np.array(i)
    print(i[1:])
    y = [ x for x in range(len(i[1:]))]
    y = np.array(y)
    plt.plot(i[1:],y,label='incident')

    if len(burst_result[i[0]-1][1]) != 1:
        for row in burst_result[i[0]-1][1]:
            print(row)
            plt.hlines(len(i[1:])/max([x[0] for x in burst_result[i[0]-1][1]] ) * row[0],row[1],row[2],color='red',linewidth='4')
    plt.legend()
    plt.savefig("burst_test{0}.png".format(i[0]))
