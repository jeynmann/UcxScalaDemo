#!/usr/bin/env sh

export UCX_LOG_LEVEL=ERROR
export UCX_ERROR_SIGNALS=""
export UCX_USE_MT_MUTEX=yes
export UCX_SOCKADDR_TLS_PRIORITY=tcp
export UCX_TLS=tcp,dc_x
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
export UCX_NET_DEVICES=mlx5_bond_0:1,bond0
export UCX_ADDRESS_VERSION=v2

size=$((4*1024))
lead=$1
bind=$2
numClient=$3
numServer=$4
numFlight=$5

arg=""
if [ $host ];then
    arg="$arg -a $host"
fi
if [ $port ];then
    arg="$arg -b $port"
fi
if [ $numClient ];then
    arg="$arg -cb $numClient"
fi
if [ $numServer ];then
    arg="$arg -s $numServer"
fi

java -cp target/ucx-0.1-for-demo-jar-with-dependencies.jar jeyn.demo.ucx.example.Demo $arg

