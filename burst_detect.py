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
import time
import numpy as np
import sqlite3
import matplotlib.pyplot as plt
import pybursts
import math
from concurrent import futures
from itertools import chain


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

class Node():
    def __init__(self,parent,st,en,lv,cnt,depth = 0):
        self.parent = parent #親
        self.st = st #データ
        self.en = en
        self.lv = lv
        self.cnt = cnt
        self.children = [] #子
        self.depth = depth

    def add_node(self,added_node): #ノード追加
        self.children.append(added_node)
        added_node.parent = self
        added_node.depth = self.depth + 1

    def dens(self):# 1min間の発生件数を返す
        if self.en - self.st == 0:
            return 0
        else:
            return round( self.cnt / (self.en - self.st) * 60 ,2 )

    def value(self):
        return [self.lv,self.st,self.en,self.cnt,self.dens()]


# p_numプロセスでバースト検知。time_listsをデータ数が多い順にp_nu個に分配して渡す。
def m_burst_detect(time_lists,p_num):
    if p_num > len(time_lists):
        p_num = len(time_lists)

    row_lists = sorted(time_lists.items(),key=lambda x:len(x[1]),reverse=True)

    arg_lists = []
    for i in range(p_num):
        arg_lists.append({k:v for e,(k,v) in enumerate(row_lists) if e%p_num == i})

    pool = futures.ProcessPoolExecutor(max_workers=p_num)
    return(list(chain.from_iterable(pool.map(burst_detect,arg_lists))))


def burst_detect(time_lists):
    burst_result = []
    for ind,v in time_lists.items():
        time_list = list(v)#参照渡しではなくコピー
        if len(time_list) > 30:#量でフィルタ

            #最初と最後が0と86400じゃなかったら臨時で追加
            if time_list[-1] < 86400:
                time_list.append(86400)
            if time_list[0] != 0:
                time_list.insert(0,0)


            # print('len',time_list[:10])
            #バースト検知
            burst_list = pybursts.pybursts.kleinberg(sorted(set(time_list)),s=2,gamma=1.0)

            # print(time_list[:20])
            print(len(set(time_list)))
            print(burst_list)

            #ここで重複レベルを削除
            for j in range(len(burst_list)-1):
                if not any([x-y for x,y in zip(burst_list[j][1:],burst_list[j+1][1:])]):#始点と終点が一緒だったら
                    burst_list[j] = [0,0,0]
            burst_list = np.delete(burst_list,np.where(burst_list == 0)[0],0)
            # print('before burst_list',burst_list)


            #ここでintervalが1min超える場合は削除
            burst_list = check_interval(burst_list,time_list)

            #バーストツリー生成開始
            root_node = Node(None,0,0,0,0) #ルートノード
            for lv,st,en in burst_list :
                #初期化
                parent_node = root_node
                isadded = 0
                burst_cnt = len([ z for z in time_list if st <= z <= en])
                new_node = Node(None,st,en,lv,burst_cnt)

                while isadded == 0:
                    for child_node in parent_node.children:#子供を順次比較していく
                        if child_node.st <= new_node.st and child_node.en >= new_node.en :#包含関係チェック
                            if child_node.children == []:#包含関係にあり、比較対象の子供がいない時はそのまま追加して終わり
                                child_node.add_node(new_node)
                                isadded = 1
                                break
                            else:
                                parent_node = child_node#包含関係にあり、比較対象の子供がいる場合は親交代して比較
                                break
                        else:#包含関係になかったら、次の子供と比較
                            pass
                    else:#どの子供とも包含関係になかったら追加して終わり
                        parent_node.add_node(new_node)
                        isadded = 1
            #バーストツリー生成終了,root_node以下に格納。

            #バーストツリー表示
            print(ind,'result')
            show_burst_tree(root_node)

            '''
            #バーストツリー走査
            parent_node = root_node
            result_node = []
            while True:
                for cur_node in parent_node.children:
                    if cur_node.children == [] :
                        result_node.append(cur_node)
                    elif any(cur_node.dens > x.dens * 2 for x in cur_node.children) : #cur_nodeの密度がどの子供の密度より2倍以上ある時
                        result_node.append(cur_node)
                    else : #半分以下の密度でない子供がいる時
            '''

            #暫定listが残っていたらresultに追加
            if len(burst_list) != 0:
                #第一層の子供の結果を全部入れる
                burst_result.append( (ind, [ z.value() for z in root_node.children ]) )
            # print(burst_result)
    return burst_result


#バーストツリー表示
def show_burst_tree(parent_node):
    for i in range(parent_node.depth):
        print('\t',end='')
    print('[',parent_node.lv,parent_node.st,parent_node.en,parent_node.cnt,parent_node.dens(),']')
    for child in parent_node.children:
        show_burst_tree(child)



#1groupのtime listを受ける。
def check_interval(burst_range,group_time_list):
    if burst_range == []:
        return burst_range
    burst_range_result = []
    sub_list = []
    # print('check interval',burst_range)

    for lv,s,e in burst_range:
        sub_list = [ y - x for x,y in zip(group_time_list[:-1],group_time_list[1:]) if s <= x <= e and s <= y <= e ]
        if max(sub_list) <= 60 * 2 :#最大インターバルが2分以内であること
            sub_list_count = collections.Counter(sub_list)
            over_1min_interval_rate = sum([x for k,x in sub_list_count.items() if k > 60]) / len(sub_list)
            # print('check interval result:',sub_list_count,over_1min_interval_rate)
            if over_1min_interval_rate < 0.5:#インターバルが1分以上のものが50%以下であること
                burst_range_result.append([lv,s,e])
            sub_list = []
        else:
            print('interval check hit',lv,s,e)

    return burst_range_result

if __name__ == '__main__':
    dbname=sys.argv[1]
    con = sqlite3.connect(dbname)
    cur = con.cursor()
    time_lists={}
    for i in range(290,295):
        print(i)
        cur.execute("""select time from '{0}'""".format(i))
        time_list = np.sort(np.array([x[0] for x in cur.fetchall()]))

        time_list = time_list - 43000

        # time_list[int(np.size(time_list)/2):] = 0
        time_lists[i] = time_list
    con.commit()
    con.close()

    # for ind,time_list in time_lists.items():
    #     # time_list = time_list.astype(np.float64)
    #     for i in range(len(time_list)):
    #         if i == 0:
    #             continue
    #         if math.floor(time_list[i-1]) == time_list[i]:#秒単位でも重複がある場合は少数3桁でカウントしていく
    #             print('hit')
    #             time_list[i] = round(time_list[i-1] + 0.0001,4)

    # print('len',time_lists[343][100:120])
    start_time = time.time()
    print(m_burst_detect(time_lists,1))
    end_time = time.time()
    print(end_time - start_time)

    # start_time = time.time()
    # pool = futures.ProcessPoolExecutor(max_workers=3)
    # a = list(pool.map(burst_detect,[{1:time_lists[1]},{2:time_lists[2]},{3:time_lists[3]}]))
    #
    # end_time = time.time()
