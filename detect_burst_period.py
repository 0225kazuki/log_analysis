#!/usr/bin/python
# coding: UTF-8

'''
周期判定

python detect_period.py xxxx.dat offset
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


TIME_OFFSET = int(sys.argv[2])

FILENAME = sys.argv[1]
FD = open(FILENAME,"r")

def sec2time(sec):
    return str(int(sec/3600)).zfill(2)+':'+str(int(sec%3600/60)).zfill(2)+':'+str(int(sec%3600%60)).zfill(2)


if __name__ == '__main__':
    time_lists = burst_detect.get_time_stamp()
    bursts = burst_detect.burst_detect(time_lists)
    bursts_dict = {k:v for k,v in bursts}


    head_log_list = []
    bottom_log_list = []

    for i,time_list in time_lists.items():
        if not isinstance(bursts_dict.get(i,0),int):
            non_burst_time = list(time_list)
            for lv,s,e in bursts_dict[i]:
                non_burst_time = [z for z in non_burst_time if s > z or e < z]
            time_lists[i] = non_burst_time

    for ind,time_list in time_lists.items():
        # if ind != 7:
        #     continue

        if len(time_list) < 24:
            maxcor = 'none'
            maxind = 0
        else:
            #0-86400secの時系列データ化
            time_series = np.zeros(24*60*60)
            for j in time_list:
                time_series[int(j)] = 1
            cor = np.correlate(time_series,time_series[:86400 - 3600])#全区間で自己共分散計算

            #0secずらしを除いて、一番最初のピークを求める。
            maxcor = 0
            maxind = 0
            for i,row in enumerate(cor[1:3601],start = 1):
                if row > maxcor:
                    maxcor = row
                    maxind = i



        '''
        ここから結果の処理
        '''
        print('result',ind)

        dbname1 = FILENAME[:-3]+'db'
        con1 = sqlite3.connect(dbname1)
        cur1 = con1.cursor()
        dbname2 = FILENAME[:-3]+'db'
        con2 = sqlite3.connect(dbname2)
        cur2 = con2.cursor()
        cur1.execute("""select * from '{0}' """.format(ind))

        isburst = 0
        isperiod = 0

        cur2.execute("""select f from format where id = {0}""".format(ind))
        temp = cur2.fetchall()[0][0]
        print(temp)
        print('burst : ',end='')
        if not isinstance(bursts_dict.get(ind,0),int):
            isburst = 1
            print(bursts_dict[ind])
            head_log_list.append(('burst',temp,bursts_dict[ind]))
        else:
            print('none')


        if maxcor == 'none':
            print('period : none','data cnt=',len(time_list))
            if isburst == 0:#バーストも周期もなかったらbottomリストに加える
                cur1.execute("""select time,log from '{0}' """.format(ind))
                time_log_list = cur1.fetchall()
                time_log_list = sorted([(t,l) for t,l in time_log_list if t in time_list])
                for sec,l in time_log_list:
                    print(sec,sec2time(sec),l)
                    bottom_log_list.append((sec,l))
        else:
            print('period :','max cor =',maxcor,'period =',maxind,'data cnt=',len(time_list))
            print(sorted({k:v for k,v in enumerate(cor[1:3601],start = 1)}.items(),key = lambda x:x[1])[:-11:-1])

            if maxind < 60:#periodでフィルタ
                isperiod = 1
                print('estimate random pattern: row period')
            else:#periodでずらしたACでフィルタ
                for offset in range(1,int(3601/maxind)):
                    if offset * maxind < 3601:
                        print(maxind * offset,cor[maxind*offset],end=',')
                        if cor[maxind*offset]/len(time_list) < 0.6:
                            print('estimate random pattern')
                            isperiod = 1

            if isperiod == 0:#周期あり
                head_log_list.append(('periodical',temp,maxind))
            else:#周期なし=ランダム
                head_log_list.append(('random',temp))
        print('\n')


    '''reduce結果の最終表示'''
    print('header',len(head_log_list),' lines')
    for row in head_log_list:
        if row[0] == 'burst':
            print('Burst detected:',row[1])
            for lv,st,en in row[2]:
                print('lv=',lv,'\t',sec2time(st),'-',sec2time(en))
        elif row[0] == 'periodical':
            print('Period detected:',row[1])
            print(row[2],'sec')
        elif row[0] == 'random':
            print('Random log:',row[1])
    print('\nbottom',len(bottom_log_list),' lines')
    for sec,log in sorted(bottom_log_list):
        print(sec2time(sec),log)
