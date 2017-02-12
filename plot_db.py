#!/usr/bin/python
# coding: UTF-8

'''
累積和、時系列、ヒストグラム生成

python plot.py xxxx.db
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

# TIME_OFFSET = int(sys.argv[2])

DBNAME = sys.argv[1]

'''
Time Stampの取得(秒換算)
'''
def get_time_stamp():
    time_lists = {}
    con = sqlite3.connect(DBNAME)
    cur = con.cursor()
    cur.execute("""select name from sqlite_master """)

    #group の数
    group_ids = [int(x[0]) for x in cur.fetchall() if x[0].isdigit()]

    for i in group_ids:
        cur.execute("""select time from '{0}'""".format(i))
        time_lists[i] = np.sort(np.array([x[0] for x in cur.fetchall()]))
    con.commit()
    con.close()
    return time_lists

if __name__ == '__main__':
    time_lists = get_time_stamp()
    # pprint.pprint(time_lists)

    for i,time_list in time_lists.items():
        # if i != 1:
        #     continue


        print('start\t id:{0} burst plot'.format(i))
        if len(time_list) < 5:
            continue
        fig = plt.figure(figsize=(18, 12))
        ax = fig.add_subplot(1,1,1)

        '''
        階段状にする処理
        x = [0,1,2,3,...] -> x = [0,1,1,2,2,3,3,...]
        y = [a,b,c,d,...] -> y = [a,a,b,b,c,c,d,d,...]
        '''
        time_list=np.append(time_list,0)
        time_list=np.append(time_list,86399)
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

        plt.plot(x,y,label='log')

        # burst_result = [(1, [[3.0, 23916, 24704, 38, 2.8900000000000001]]), (2, [[3.0, 63985, 66980, 116, 2.3199999999999998]]), (3, [[3.0, 19712, 20756, 47, 2.7000000000000002], [3.0, 33615, 34166, 29, 3.1600000000000001]]), (4, [[3.0, 63584, 64886, 68, 3.1299999999999999]]), (5,[]),(6, [[3.0, 54717, 56061, 56, 2.5], [3.0, 58219, 59545, 65, 2.9399999999999999]]), (7, [[3.0, 41621, 42291, 35, 3.1299999999999999], [3.0, 49591, 53942, 169, 2.3300000000000001]]), (8, [[3.0, 60526, 61220, 37, 3.2000000000000002], [3.0, 65930, 70279, 165, 2.2799999999999998]]), (9, [[3.0, 21999, 22569, 32, 3.3700000000000001], [3.0, 24207, 28319, 148, 2.1600000000000001]])]
        #
        # for lv,s,e,cnt,dns in burst_result[i-1][1]:
        #     plt.hlines(max(y)/2,s,e,color='red',linewidth='4')

        # グリッドを表示
        plt.grid(True)

        #タイトル表示
        con = sqlite3.connect(DBNAME)
        cur = con.cursor()
        cur.execute("""select f from format where id = {0} """.format(i))
        title = cur.fetchall()
        con.commit()
        con.close()
        # plt.title(title,fontsize='22')
        # plt.title('3 times burst(60min, 1.0cnt/min)',fontsize='22')

        plt.xlim(0,86400)

        sec_list = [z*60*60 for z in range(0,24) if z%2 == 0]
        label_list = [str(z)+':00' for z in range(0,24) if z%2 == 0]
        plt.xticks(sec_list,label_list)
        plt.xlabel("time",fontsize='30')
        plt.ylabel("count",fontsize='30')
        plt.tick_params(labelsize=22)
        # plt.legend()
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
        # print(interval_cnt)

        x = [z[0] for z in interval_cnt]
        y = [z[1] for z in interval_cnt]

        # print(x)
        # print(y)
        # plt.ylim(0,30)###########
        if max(x) < 30:
            plt.xlim([-5,30])
        else:
            plt.xlim([-5,max(x)+200])
        plt.bar(x,y,label='log',color='red',edgecolor='red',width=1.0)

        plt.title(title,fontsize='22')
        plt.grid(True)

        plt.xlabel("time(sec)",fontsize='30')
        plt.ylabel("count",fontsize='30')
        plt.tick_params(labelsize=22)
        plt.legend()
        plt.savefig("{0}hist.png".format(i))


        # # 1h ts plot
        # plt.figure(figsize=(18, 12))
        # x = time_list
        # y = [1 for x in x]
        # plt.bar(x,y,label='log',color='red',edgecolor='red',width=1.0)
        # plt.tick_params(labelsize=22)
        # plt.legend()
        #
        # for j in range(24):
        #     plt.xlim(60*60*j,60*60*(j+1))
        #     plt.ylim(0,2)
        #     plt.savefig("{0}ts{1}-{2}.png".format(i,j,j+1))
