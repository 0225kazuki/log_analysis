#!/usr/bin/python
# coding: UTF-8

import collections
import pprint
import re
import sys
import numpy as np
import sqlite3
import Levenshtein
import datetime
import time

HEADER_OFFSET = 4
parse_char = ['(',')','[',']','=']

'''
start_time = time.time()
end_time = time.time()
print('xxxx:',end_time - start_time)
'''

node_num = 0

class Node():
    def __init__(self,parent,value):
        self.parent = parent #親
        self.value = value #データ
        self.c_node = [] #子

    def add_node(self,add_node): #ノード追加
        global node_num
        #print("add node")
        if len(self.c_node) == 64: #子ノード数上限なら弾く
            return
        else:
            # print('node_num = ',node_num)
            node_num += 1
            self.c_node.append(add_node)
            add_node.parent = self


class Parameter():
    def __init__(self,num,trend):
        self.num = num
        self.trend = trend
        self.name = None

class FormatLog():
    def __init__(self,f,cnt):
        self.format = f
        self.trend = self.mk_trend()
        self.cnt = cnt

    def mk_trend(self):
        trend = []
        for a in self.format:
            if isinstance(a,Parameter):
                trend.append(a.trend)
                # print('mk_trend para',a.trend)
            else:
                trend.append(word_coord(a))
        return trend


def word_split(log):
    #単語分割
    w = list(log)
    for (i,word) in enumerate(w):
        if word in parse_char:
            w[i] = ' ' + w[i] + ' '
    w = ''.join(w)
    w = re.split(' +',w[:-1])
    if w[-1] == '':
        w = w[:-1]
    return w[HEADER_OFFSET:]

def word_coord(w):#単語 or listが入ってくる
    if isinstance(w,list):
        trend = np.zeros(28)
        trend_list = []
        for word in w:#word分割
            wchar = word
            if wchar == '':
                continue
            for c in wchar:#char分割
                if c.isalpha():
                    ascii_no = ord(c.lower())
                    trend[ascii_no - 97] += 1
                elif c.isdigit():#数字の場合
                    trend[26] += 1
                else :#記号の場合
                    trend[27] += 1
            trend = trend/np.linalg.norm(trend)
            trend_list.append(trend)
            trend = np.zeros(28)
        return trend_list
    else:
        trend = np.zeros(28)
        if w == '':
            return trend
        for c in w:#char分割
            if c.isalpha():
                ascii_no = ord(c.lower())
                trend[ascii_no - 97 ] += 1
            elif c.isdigit():#数字の場合
                trend[26] += 1
            else :#記号の場合
                trend[27] += 1
        trend = trend/np.linalg.norm(trend)
        return trend


def seqratio(w1,w2):
    w1_trend = word_coord(w1)
    if isinstance(w2,FormatLog):
        w2_trend = w2.trend
    else:
        w2_trend = word_coord(w2)

    if len(w1_trend) != len(w2_trend):
        return 0
    else:
        r = 0
        aaa = 0
        for (a,b) in zip(w1_trend,w2_trend):
            dist = np.linalg.norm(b-a)
            r += dist
        sr = 1.0 - r/(2*len(w1_trend))
        return sr

def make_format(w1,w2=None):
    if w2 == None:
        return FormatLog(w1,0)
    else:
        format_str = []
        if(isinstance(w2,FormatLog)):
            integrate_cnt = w2.cnt + 1
            w2 = w2.format
        else:
            integrate_cnt = 0
        for (a,b) in zip(w1,w2):
            i = 0
            if a != b :
                p_trend = word_coord(a)
                p = Parameter(i,p_trend)
                i += 1
                format_str.append(p)
            else:
                format_str.append(a)

        f = FormatLog(format_str,integrate_cnt)
        return f

def sim(w1,w2):
    format_w1 = []
    format_w2 = []
    if isinstance(w1,list):
        format_w1 = w1
    else:
        for row in w1.format:
            if isinstance(row,Parameter):
                format_w1.append("*")
            else:
                format_w1.append(row)

    if isinstance(w2,list):
        format_w2 = w2
    else:
        for row in w2.format:
            if isinstance(row,Parameter):
                format_w2.append("*")
            else:
                format_w2.append(row)

    format_w1 = " ".join(format_w1)
    format_w2 = " ".join(format_w2)
    dis = Levenshtein.distance(format_w1,format_w2)
    sim_r = dis * 2 / (len(format_w1)+len(format_w2))
    return sim_r

def get_datetime_str():
    return datetime.datetime.now().strftime('%y%m%d_%H%M%S')


def create_db(nparent,dbname):
    con = sqlite3.connect("{0}.db".format(dbname) )
    cur = con.cursor()
    cur.execute("""create table if not exists Node (id integer primary key,f text,cnt integer)""")
    # con.execute("PRAGMA busy_timeout = 30000")
    for node_row in nparent.c_node:
        format_list = []
        if isinstance(node_row.value,list):
            format_list = node_row.value
        else:
            for row in node_row.value.format:
                if isinstance(row,Parameter):
                    format_list.append("*")
                else:
                    format_list.append(row)

            for (i,word) in enumerate(format_list):#エスケープ処理
                if "'" in word:
                    format_list[i] = format_list[i].replace("'", "''")
            # print('formatlist = ',format_list)
            cur.execute("""insert into Node(f,cnt) values ('{0}',{1})""".format( " ".join(format_list),node_row.value.cnt) )
    con.commit()
    con.close()

    for node_row in nparent.c_node:
        if len(node_row.c_node) > 0 :
            create_db(node_row,dbname)


cnt = 0
root_node = Node(None, None)


filename = sys.argv[1]
fd = open(filename)
log = fd.readline()


'''start_time = time.time()
end_time = time.time()
print('xxxx:',end_time - start_time)
'''
dbname = sys.argv[1].split('/')[1]
print(dbname)
while log:
    #Algorithm 1
    # start_time = time.time()
    if cnt%100 == 0:
        print('cnt:',cnt)

    start_time1 = time.time()
    n = Node(None,word_split(log))
    # end_time1 = time.time()
    # print('create node:',end_time1 - start_time1)
    # print ('\ninput ',log,n.value)
    f = None
    nparent = root_node
    # print('parent-child',nparent.c_node)
    t = 0.95
    while f == None:
        # start_time2 = time.time()
        for nchild in nparent.c_node:#子ノードと新規ログを比較
            if seqratio(n.value,nchild.value) > t:#seqratioが閾値超えたらマージして終わり。
                f = make_format(n.value,nchild.value)
                nchild.value = f
                break
        # end_time2 = time.time()
        # print('search tree:',end_time2 - start_time2)


        # start_time3 = time.time()
        if f == None:
            if len(nparent.c_node) < 64:
                f = make_format(w1 = n.value)
                n.value = f
                nparent.add_node(n)
            else:
                r = 1
                for nchild in nparent.c_node:#simは低いほど近い
                    sim_r = sim(nchild.value,n.value)
                    if sim_r < r:
                        next_parent = nchild
                        r = sim_r
                if r == 1:#どのノードともsimrateが0となった場合
                    next_parent = nparent.c_node[0]
                nparent = next_parent

        # end_time3 = time.time()
        # print('search tree2:',end_time3 - start_time3)

    cnt +=1
    # if cnt > 200000:
    #     break;
    log = fd.readline()
    # end_time = time.time()
    # print('1 transaction:',end_time - start_time)
fd.close()

create_db(root_node,dbname)
print("job finished:",filename)
