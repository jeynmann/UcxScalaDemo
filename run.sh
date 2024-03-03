#!/usr/bin/env sh

#  sh run.sh '' 3000 1 500 1
#  sh run.sh 192.168.20.10:3000 '' 1 500 1

export JAVA_HOME=/usr/lib/jvm/TencentKona-8.0.6-292
export JAVA_LIBRARY_PATH=$JAVA_HOME/jre/lib:$JAVA_HOME/jre/lib/amd64:$JAVA_HOME/jre/lib/amd64/jli:$JAVA_HOME/jre/lib/amd64/server
export JAVA_OPTS="-Dlog4j.configurationFile=log4j2.xml -XX:MetaspaceSize=40m -XX:+PrintCommandLineFlags -Xmx10g -Xms10g -Djava.library.path=$JAVA_LIBRARY_PATH -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:/tmp/gc.log -verbose:gc -XX:SurvivorRatio=4 -XX:+UseMembar -XX:+UseCompressedOops -XX:+IgnoreUnrecognizedVMOptions -XX:+IgnoreNoShareValue -XX:CPUShareScaleFactor=6 -XX:CPUShareScaleLimit=15"

export UCX_LOG_LEVEL=error
export UCX_ERROR_SIGNALS=""
export UCX_USE_MT_MUTEX=yes
export UCX_SOCKADDR_TLS_PRIORITY=tcp
export UCX_TLS=tcp
export UCX_DC_MLX5_NUM_DCI=32
export UCX_DC_TX_CQE_ZIP_ENABLE=yes
export UCX_DC_RX_CQE_ZIP_ENABLE=yes
export UCX_DC_MLX5_DCT_PORT_AFFINITY=random
export UCX_IB_PCI_RELAXED_ORDERING=on
export UCX_RC_SRQ_TOPO=cyclic_emulated
export UCX_MAX_RNDV_RAILS=2
export UCX_ZCOPY_THRESH=500
export UCX_RNDV_THRESH=500
export UCX_KEEPALIVE_INTERVAL=inf
export UCX_ADDRESS_VERSION=v2

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

