#!/usr/bin/python
# coding: UTF-8

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

#時系列の累積和に対してバースト検知
offsets = [4, 17, 23, 27, 33, 35, 37, 76, 77, 82, 84, 88, 90, 92]
# print pybursts.kleinberg(offsets, s=2, gamma=0.1)

filename = sys.argv[1]
fd = open(filename,"r")
data = [x[:-1].split(',') for x in fd.readlines()]
fd.close()

time_list = []
for i in data:
    time_stamp = i[0].split()[3].split(':')
    time_list.append(int(time_stamp[0])*60*60+int(time_stamp[1])*60+int(time_stamp[2]))


# print(time_list)

# print(sorted(list(set(data))))
burst_result = pybursts.pybursts.kleinberg(sorted(set(time_list)),s=2,gamma=1.0)
print(burst_result,len(burst_result))


plt.figure(figsize=(40, 12))
# data = [x[:-1].split(',') for x in fd.readlines()]
# y = [ x for x in range(len(time_list))]
plt.plot(time_list,[ x for x in range(len(time_list))],label='invalid incident')
for row in burst_result[1:]:
    plt.hlines(len(time_list)/6*row[0],row[1],row[2],color='red',linewidth='4')

plt.legend()
plt.savefig("burst_test{0}.png".format(filename[0:-4].split('/')[1]))
