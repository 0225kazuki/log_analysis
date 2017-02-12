#!/usr/bin/python
# coding: UTF-8

'''
バースト&周期判定

python detect_burst_period.py xxxx.db offset
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
from concurrent import futures
from itertools import chain


# TIME_OFFSET = int(sys.argv[2])

FILENAME = sys.argv[1]
FD = open(FILENAME,"r")

def sec2time(sec):
    return str(int(sec/3600)).zfill(2)+':'+str(int(sec%3600/60)).zfill(2)+':'+str(int(sec%3600%60)).zfill(2)

def get_time_stamp():
    dbname = FILENAME
    time_lists = {}
    con = sqlite3.connect(dbname)
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

# p_numプロセスで周期検知。time_listsをデータ数が多い順にp_nu個に分配して渡す。
def m_period_analysis(time_lists,p_num):
    if p_num > len(time_lists):
        p_num = len(time_lists)

    row_lists = sorted(time_lists.items(),key=lambda x:len(x[1]),reverse=True)

    arg_lists = []
    for i in range(p_num):
        arg_lists.append({k:v for e,(k,v) in enumerate(row_lists) if e%p_num == i})

    pool = futures.ProcessPoolExecutor(max_workers=p_num)
    a = list(pool.map(period_analysis,arg_lists))
    # result = {k:v for s in a for k,v in s.items()}

    return({k:v for s in a for k,v in s.items()})

def period_analysis(time_lists):
    period_analysis_result = {} # {index : [ period , israndom , irregular_time ]}
    for ind,time_list in time_lists.items():
        if len(time_list) < 10: #2時間周期でも12件はある。1時間周期24件。
            period_analysis_result[ind] = [0,0]
            continue

        else:
            #0-86400secの時系列データ化
            time_series = np.zeros(24*60*60)
            for j in time_list:
                time_series[int(j)] = 1

            #calculate interval, select mode interval(>10sec)
            interval_list = [ round((t2 - t1)/10)*10 for t1,t2 in zip(time_list[:-1],time_list[1:]) ]
            interval_cnt = sorted(collections.Counter(interval_list).items(),key=lambda x:x[1],reverse=True)

            #インターバルを頻度順に見ていき10sec以上のものを候補として採用、最大2時間
            interval_modes = [(int(k),v) for k,v in interval_cnt if 10 <= k <= 60*60*2]

            for interval_mode,interval_mode_cnt in interval_modes:
                print('ID: ',ind)
                print('mode = {0}({1}%) , {2} sec'.format(interval_mode_cnt,round(interval_mode_cnt/len(interval_list)*100,2),interval_mode))

                if interval_mode_cnt/len(interval_list) > 0.9: #候補周期が全体の9割だったらそのまま採用
                    period = interval_mode
                    print('period = ',period)
                    cor = []
                    break
                elif interval_mode_cnt/(86400/interval_mode) < 0.5: #候補周期で24h出た時の総数に対する実際の発生件数の割合で閾値
                    continue

                #理想周期配列との相関係数をとる
                interval_mode_ts = np.zeros(60 * 60 * 12)
                interval_mode_ts[0] = 1
                for i in range(1,int(60 * 60 * 12 / interval_mode)):
                    interval_mode_ts[i * interval_mode] = 1 #理想周期配列生成(12h分)

                cor = np.correlate(time_series[time_list[0]:],interval_mode_ts)#12h分で相関係数

                print('cor top 10',cor[:10])
                print('maxcor = ',max(cor),np.where(cor==max(cor))[0][0])
                print('all cnt = ',int(60 * 60 * 12 / interval_mode))
                maxcor_ind = np.where(cor == max(cor))[0][0]
                # print('forward:',[cor[maxcor_ind+interval_mode*z] for z in range(100) if maxcor_ind+interval_mode*z<43200])
                # print('backward:',[cor[maxcor_ind-interval_mode*z] for z in range(100) if maxcor_ind-interval_mode*z>0])

                if interval_mode != 0 and max(cor)/(60*60*12/interval_mode) > 0.5:#ヒット率が半分超えたら周期的と判定
                    maxcor_ind = np.where(cor == max(cor))[0][0]
                    period = interval_mode
                    print('period = ',period)
                    break

            else: #interval_modeが0だったら
                period = 0
                print('non periodical')


            #irregular point detection
            irregular_time = []
            if period != 0 and cor != []:
                #先頭位置を決定
                th = max(cor)*0.5
                for sec,row in enumerate(cor):
                    if row > th:
                        start_cur = sec + time_list[0]
                        print('start_cur=',start_cur,row)
                        break
                for z in range(int((24*60*60-start_cur)/period)):
                    if time_series[start_cur + z*period] == 0:
                        irregular_time.append(start_cur+z*period)


            #Random判定
            israndom = 0
            if period == 0:
                hourly_cnt = [ sum(time_series[z*60*60 : (z+1)*60*60]) for z in range(24)]
                print('hourly_cnt',hourly_cnt,len(time_list))
                if len(time_list) > 20:#20件以上あったらランダムとして集約
                    israndom = 1
                    all_random_logs = sum(hourly_cnt)
                    hourly_avg = all_random_logs/len(hourly_cnt)

            if israndom != 1:
                period_analysis_result[ind] = [period,israndom,irregular_time]
            elif israndom == 1:
                period_analysis_result[ind] = [period,israndom,all_random_logs,hourly_avg]
    return period_analysis_result

# tmps = [(tmp1,),(tmp2,),(tmp3,)]
# all=1で全テンプレート表示
def print_tmps(tmps,all=0):
    if all == 1:
        for tmp in tmps:
            print(tmp[0])
    else:
        most = sorted([(tmp[0],collections.Counter(tmp[0])['*']) for tmp in tmps],key=lambda x:x[1],reverse=True)
        print(most[0][0])
    return 0

if __name__ == '__main__':

    time_lists = get_time_stamp()


    # bursts = burst_detect.burst_detect(time_lists)
    bursts = burst_detect.m_burst_detect(time_lists,3)

    print('bursts',bursts)
    bursts_dict = {k:v for k,v in bursts}# { id : [ [lv,st,en,cnt,dens], ]}

    head_log_list = []
    bottom_log_list = []

    #kleinberg's Burst Detect:バースト部分はtime_listsから抜き出し
    for i,time_list in time_lists.items():
        if not isinstance(bursts_dict.get(i,0),int):
            non_burst_time = list(time_list)
            for lv,s,e,cnt,dens in bursts_dict[i]:
                non_burst_time = [z for z in non_burst_time if s > z or e < z]
            time_lists[i] = non_burst_time

    st = time.time()
    period_analysis_result = m_period_analysis(time_lists,3)
    en = time.time()
    print(en-st)

    # print(period_analysis_result)


    '''
    ここから結果の処理
    head_log_listとbottom_log_listに格納していく
    Burst  ->  head_log_list = [(type,temp,bursts_list)]
    Period ->  head_log_list = [(type,temp,period,irregulars)]
    Random ->  head_log_list = [(type,temp)]
    other  ->  bottom_log_list = [(time,temp)]
    '''

    print('\nRESULT')
    for ind,time_list in time_lists.items():
        con = sqlite3.connect(FILENAME)
        cur = con.cursor()

        cur.execute("""select f from format where id = {0}""".format(ind))
        # temp = cur.fetchall()[0][0]
        temp = cur.fetchall()

        if not isinstance(bursts_dict.get(ind,0),int):#burst性あり
            head_log_list.append(('burst',temp,bursts_dict[ind]))
        if period_analysis_result[ind][0] != 0:#周期あり
            head_log_list.append(('periodical',temp,period_analysis_result[ind][0],period_analysis_result[ind][2]))
        elif period_analysis_result[ind][1] == 1:#random性あり
            head_log_list.append(('random',temp,period_analysis_result[ind][2],period_analysis_result[ind][3]))
        else:#others
            cur.execute("""select time,log from '{0}' """.format(ind))
            time_log_list = cur.fetchall()
            time_log_list = sorted([(t,l) for t,l in time_log_list if t in time_list])
            for sec,l in time_log_list:
                bottom_log_list.append((sec,l))

        print('\nID',ind)
        print(temp)
        if bursts_dict.get(ind,0) != 0:
            print(bursts_dict[ind])
        else:
            print('no burst')
        print(period_analysis_result[ind][:2])



        # head_log_list = sorted(head_log_list)

        con.close()

    '''reduce結果の最終表示'''
    print('\nTop: Log Summary (',len(head_log_list),' blocks)')
    head_log_list = sorted(head_log_list)
    for row in head_log_list:
        if row[0] == 'burst':
            # print('Burst detected:',row[1])
            print('Burst detected:')
            print_tmps(row[1])
            for lv,st,en,cnt,dens in row[2]:
                print('\t',sec2time(st),'-',sec2time(en),'\t',cnt,'\t',dens,'cnt/min')
        elif row[0] == 'periodical':
            print('Period detected:')
            print_tmps(row[1])
            print('\t',row[2],'sec')
            # if row[3] != []:
            #     print('Irregular points:',row[3])
        elif row[0] == 'random':
            print('Random log:')
            print_tmps(row[1])
            print('\t',row[2],'cnt/hour\t',row[3])
        print('\n')
    print('\nBottom: Row Log messages (',len(bottom_log_list),' lines)')
    for sec,log in sorted(bottom_log_list):
        print(sec2time(sec),log)
