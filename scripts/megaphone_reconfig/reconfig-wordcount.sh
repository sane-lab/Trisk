#!/bin/bash

### ### ###  		   ### ### ###

### ### ### INITIALIZATION ### ### ###

### ### ###  		   ### ### ###

init() {
  # app level
  FLINK_DIR="/path/to/Flink"
  FLINK_APP_DIR="/path/to/Flink_app"
  JAR=${FLINK_APP_DIR}$"target/testbed-1.0-SNAPSHOT.jar"
  ### paths configuration ###
  FLINK=$FLINK_DIR$"bin/flink"
  JOB="megaphone.dynamicrules.Main"

  # kafka related configs
  samza_path="/samza-hellow-samza-path"
  GRID=$samza_path"/bin/grid"

  runtime=200
}

restart_kafka() {
  $GRID stop kafka
  $GRID stop zookeeper
  kill -9 $(jps | grep Kafka | awk '{print $1}')
  rm -r /tmp/kafka-logs/
  rm -r /tmp/zookeeper/
  python -c 'import time; time.sleep(5)'
  $GRID start zookeeper
  $GRID start kafka
}

# run flink clsuter
function runFlink() {
    echo "INFO: starting the cluster"
    if [[ -d ${FLINK_DIR}log ]]; then
        rm -rf ${FLINK_DIR}log
    fi
    mkdir ${FLINK_DIR}log
    ${FLINK_DIR}/bin/start-cluster.sh
}

# clean app specific related data
function cleanEnv() {
  if [[ -d ${FLINK_DIR}${EXP_NAME} ]]; then
      rm -rf ${FLINK_DIR}${EXP_NAME}
  fi
  mv ${FLINK_DIR}log ${FLINK_DIR}${EXP_NAME}
  rm -rf /tmp/flink*
  rm ${FLINK_DIR}log/*
}

# clsoe flink clsuter
function stopFlink() {
    echo "INFO: experiment finished, stopping the cluster"
    PID=`jps | grep CliFrontend | awk '{print $1}'`
    if [[ ! -z $PID ]]; then
      kill -9 ${PID}
    fi
    ${FLINK_DIR}bin/stop-cluster.sh
    echo "close finished"
    cleanEnv
}


# run applications
function runApp() {
  echo "INFO: $FLINK run -c ${JOB} ${JAR} &"
  rm nohup.out
  nohup $FLINK run -c ${JOB} ${JAR} &
}


# run one flink demo exp, which is a word count job
run_one_exp() {
  # compute n_tuples from per task rates and parallelism
  EXP_NAME=megaphone_exp

  echo "INFO: run exp ${EXP_NAME}"
#  configFlink
  restart_kafka
  runFlink
  python -c 'import time; time.sleep(5)'

  runApp

  SCRIPTS_RUNTIME=`expr ${runtime} + 10`
  python -c 'import time; time.sleep('"${SCRIPTS_RUNTIME}"')'
  stopFlink
}

init
run_one_exp