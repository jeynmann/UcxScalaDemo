#!/usr/bin/env bash

# cd $(dirname `readlink -f $0`)

#  sh run.sh '' 3000 1 16 1
#  sh run.sh 192.168.20.10:3000 '' 16 1 1

size=$((4*1024))
lead=$1
bind=$2
numClient=$3
numServer=$4
numFlight=$5
log=$6

arg="$arg -q 0" 
arg="$arg -x 1"
arg="$arg -d $size"
if [ "$lead" ];then
    arg="$arg -l ${lead} -a ${lead}"
fi
if [ "$bind" ];then
    arg="$arg -b ${bind}"
fi
arg="$arg -c ${numClient:=1}"
arg="$arg -s ${numServer:=1}"
arg="$arg -f ${numFlight=1}"

if [ $log ];then
    echo -e run "\e[031mjps|grep demo|cut -d ' ' -f 1|xargs kill\e[0m" if kill
    java $JAVA_OPTS -cp target -jar \
        target/ucx-demo-0.1-for-default-jar-with-dependencies.jar $arg \
 &>$log &
else
    java $JAVA_OPTS -cp target -jar \
        target/ucx-demo-0.1-for-default-jar-with-dependencies.jar $arg
fi

