#!/bin/sh

path=$1
offset=$2
fn=`echo $1 | awk -F/ '{print $NF}'`
date=`echo $path | awk -F/ '{print $(NF-1)}'`

echo $path
echo $fn
echo $date
echo $offset

python shiso1.py $path

echo "shiso1 finished"

python shiso3.py ${date}.db

echo "shiso3 finished"

python classification.py ${date}.db ${path} ${offset}

echo "classification finished"

python detect_burst_period.py ${date}.db  $offset > ${date}.txt

echo "reduce finished"
