#!/bin/sh

sec=$1
h=`expr ${sec} / 3600`
m=`expr \( ${sec} % 3600 \) / 60`
s=`expr ${sec} - ${h} \* 3600 - ${m} \* 60`
echo "${h}:${m}:${s}"
