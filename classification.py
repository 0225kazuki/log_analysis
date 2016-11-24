#!/usr/bin/python
# coding: UTF-8

import collections
import pprint
import re
import sys
import numpy as np
import sqlite3
import ssdeep
import Levenshtein
import time
from functools import reduce

HEADER_OFFSET = 4
parse_char = ['(',')','[',']','=']

def word_split(log):
    #単語分割
    w = list(log)
    for (i,word) in enumerate(w):
        if word in parse_char:
            w[i] = ' ' + w[i] + ' '
    w = ''.join(w)
    w = re.split(' +',w)
    if w[-1] == '':
        w = w[:-1]
    return w[HEADER_OFFSET:]

def get_ft():
    dbname = sys.argv[1]
    con = sqlite3.connect(dbname)
    cur = con.cursor()
    cur.execute("""select * from format""")
    data = cur.fetchall()
    data = [ [data[i][0],data[i][1].strip() ] for i in range(len(data)) ]
    con.commit()
    con.close()
    return data

#0で一致、0以外で不一致
def compare_f(log,fmt):
    l = word_split(log)
    f = fmt.split()
    if len(l) != len(f):#まず長さで評価
        return 1
    flag = 0
    for (lw,fw) in zip(l,f):
        if fw == '*':
            continue
        elif lw != fw:
            flag +=1
    return flag


if __name__ == '__main__':
    start_time = time.time()
    ft = get_ft()#ft = [[group id, format]]

    #log file open
    filename = sys.argv[2]
    fd = open(filename)
    log = fd.readline()

    #キーがグループID、valueにログのリスト。
    group_log_list = {k:[] for k in range(1,ft[-1][0]+1)}
    while log:
        log = log.strip()
        print('log = ',log)

        for group_id,fmt in ft:
            d = compare_f(log,fmt)
            if d == 0:
                print('match:',group_id,fmt,log)
                group_log_list[group_id].append(log)
                flg = 1
                break
        log = fd.readline()
    fd.close()

    end_time = time.time()
    print('time:',end_time - start_time)

    outputname = sys.argv[1].split('.')[0]+'.dat'
    fd = open(outputname,"w")

    for k in range(1,ft[-1][0]+1):
        fd.write('group {0}\n'.format(k))
        for row in group_log_list[k]:
            fd.write(row)
            fd.write('\n')

    fd.close()
