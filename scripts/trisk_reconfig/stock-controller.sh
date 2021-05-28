#!/bin/bash

FLINK_DIR="/path/to/Flink"
FLINK_APP_DIR="/path/to/Flink_app"

EXP_DIR="/data"

# run flink clsuter
function runFlink() {
    echo "INFO: starting the cluster"
    if [[ -d ${FLINK_DIR}/log ]]; then
        rm -rf ${FLINK_DIR}/log
    fi
    mkdir ${FLINK_DIR}/log
    ${FLINK_DIR}/bin/start-cluster.sh
}

# clean app specific related data
function cleanEnv() {
  if [[ -d ${FLINK_DIR}/${EXP_NAME} ]]; then
      rm -rf ${FLINK_DIR}/${EXP_NAME}
  fi
  mv ${FLINK_DIR}/log ${FLINK_DIR}/${EXP_NAME}
  rm -rf /tmp/flink*
  rm ${FLINK_DIR}/log/*
}


# clsoe flink clsuter
function stopFlink() {
    echo "INFO: experiment finished, stopping the cluster"
    PID=`jps | grep CliFrontend | awk '{print $1}'`
    if [[ ! -z $PID ]]; then
      kill -9 ${PID}
    fi
    ${FLINK_DIR}/bin/stop-cluster.sh
    echo "close finished"
    cleanEnv
}

function configFlink() {
    # set user requirement
    sed 's/^\(\s*trisk.reconfig.operator.name\s*:\s*\).*/\1'"$operator"'/' ${FLINK_DIR}/conf/flink-conf.yaml > tmp1
    sed 's/^\(\s*trisk.controller\s*:\s*\).*/\1'"$controller"'/' tmp1 > ${FLINK_DIR}/conf/flink-conf.yaml
    rm tmp1
}

# run applications
function runApp() {
  echo "INFO: ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} -p2 ${parallelism} &"
  ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} -p2 ${parallelism} &
}


# run one flink demo exp, which is a word count job
run_one_exp() {
  # compute n_tuples from per task rates and parallelism
  EXP_NAME="trisk-stock-controller"

  echo "INFO: run exp ${EXP_NAME}"
  configFlink
  runFlink

  python -c 'import time; time.sleep(5)'

  runApp

  SCRIPTS_RUNTIME=`expr ${runtime} + 10`
  python -c 'import time; time.sleep('"${SCRIPTS_RUNTIME}"')'

  stopFlink

  python -c 'import time; time.sleep(5)'
}

# initialization of the parameters
init() {
  # app level
  JAR="${FLINK_APP_DIR}/target/testbed-1.0-SNAPSHOT.jar"
  job="stock.InAppStatefulStockExchange"
  controller="StockController"
  runtime=500
  parallelism=10
  operator="MatchMaker FlatMap"
}

# run the micro benchmarks
run() {
  init
  run_one_exp
}

run
