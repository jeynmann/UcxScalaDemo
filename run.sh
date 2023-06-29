#!/usr/bin/env sh

#  sh run.sh '' 3000 1 500 1
#  sh run.sh 192.168.20.10:3000 '' 1 500 1

export JAVA_HOME=/usr/lib/jvm/TencentKona-8.0.6-292
export JAVA_LIBRARY_PATH=$JAVA_HOME/jre/lib:$JAVA_HOME/jre/lib/amd64:$JAVA_HOME/jre/lib/amd64/jli:$JAVA_HOME/jre/lib/amd64/server
export JAVA_OPTS="-Dlog4j.configurationFile=log4j2.xml -XX:MetaspaceSize=40m -XX:+PrintCommandLineFlags -Xmx10g -Xms10g -Djava.library.path=$JAVA_LIBRARY_PATH -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:/tmp/gc.log -verbose:gc -XX:SurvivorRatio=4 -XX:+UseMembar -XX:+UseCompressedOops -XX:+IgnoreUnrecognizedVMOptions -XX:+IgnoreNoShareValue -XX:CPUShareScaleFactor=6 -XX:CPUShareScaleLimit=15"

host=$1
port=$2
numClient=$3
numServer=$4
numFlight=$5

scala \
    target/ucx-demo-0.1-for-default-jar-with-dependencies.jar \
    s=${host:=} \
    p=${port:=3000} \
    cli=${numClient:=1} \
    srv=${numServer:=1} \
    f=${numFlight=1} \

