#!/usr/bin/python
# coding: UTF-8

'''
累積和、時系列、ヒストグラム生成

python plot.py xxxx.dat
'''

import collections
import pprint
import re
import sys
import numpy as np
import sqlite3
import matplotlib.pyplot as plt
import pybursts
import math
import time

DATE_OFFSET = 2
TIME_OFFSET = 3

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

pprint.pprint(time_lists)


for i,time_list in time_lists.items():
    if i != 38:
        continue
    print('start\t id:{0} burst plot'.format(i))
    if len(time_list) < 5:
        continue
    plt.figure(figsize=(18, 12))


    '''
    階段状にする処理
    x = [0,1,2,3,...] -> x = [0,1,1,2,2,3,3,...]
    y = [a,b,c,d,...] -> y = [a,a,b,b,c,c,d,d,...]
    '''
    x_cnt_dict = [(k,v) for k,v in sorted(collections.Counter(time_list).items())]
    x = np.array(list(set(time_list)))
    x_hist = np.array(list(set(time_list)))
    x = np.sort(np.append(x,x))
    y = [ v for k,v in x_cnt_dict]
    for j,row in enumerate(y[1:],start = 1):
        y[j] = y[j-1]+row

    y = np.array(y)
    y = np.append(y,y[:-1])
    y = np.append(y,0)
    y = np.sort(y)

    # burst_result = []
    #
    # burst_list = pybursts.pybursts.kleinberg(sorted(set(x)),s=2,gamma=1.0)
    # print('burst:',burst_list)
    #         # for j in range(len(burst_list)-1):
    #         #     if not any([x-y for x,y in zip(burst_list[j][1:],burst_list[j+1][1:])]):
    #         #         burst_list[j] = [0,0,0]
    #         # burst_list = np.delete(burst_list,np.where(burst_list == 0)[0],0)
    #         # if len(burst_list) != 0:
    #         #     burst_result.append( [ i[0], burst_list ])

    plt.plot(x,y,label='log')
    plt.tick_params(labelsize=22)
    plt.legend()
    plt.savefig("{0}burst.png".format(i))

    plt.figure(figsize=(18, 12))

    print('end\t id:{0} burst plot'.format(i))

    print('start\t id:{0} histo plot'.format(i))
    if len(x) < 2:
        continue

    #calculate interval
    interval_list = []
    sub_list = []
    x_hist = np.sort(x_hist)
    # interval_list = [ math.floor( (t2 - t1)/10 ) * 10 for t1,t2 in zip(x_hist[:-1],x_hist[1:]) ]
    interval_list = [ t2 - t1 for t1,t2 in zip(x_hist[:-1],x_hist[1:]) ]


    interval_cnt = sorted(collections.Counter(interval_list).items())
    print(interval_cnt)

    x = [z[0] for z in interval_cnt]
    y = [z[1] for z in interval_cnt]

    print(x)
    print(y)
    # plt.ylim(0,30)###########
    if max(x) < 30:
        plt.xlim([-5,30])
    else:
        plt.xlim([-5,max(x)+200])
    plt.bar(x,y,label='log',color='red',edgecolor='red',width=1.0)
    plt.tick_params(labelsize=22)
    plt.legend()
    plt.savefig("{0}hist.png".format(i))

    print('end\t id:{0} burst plot'.format(i))

    # A = np.array([non_burst_time,np.ones(len(non_burst_time))])
    # A = A.T
    # a,b = np.linalg.lstsq(A,y)[0]
    #
    # non_burst_time = np.array(non_burst_time)
    #
    # plt.plot(non_burst_time,(a*non_burst_time+b),"g--")
    #
    # if len(burst_result[i[0]-1][1]) != 1:
    #     for row in burst_result[i[0]-1][1]:
    #         plt.hlines(len(i[1:])/max([z[0] for z in burst_result[i[0]-1][1]] ) * row[0],row[1],row[2],color='red',linewidth='4')
    # plt.legend()
    # plt.savefig("burst_test{0}.png".format(i[0]))
