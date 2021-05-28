#!/bin/bash

### ### ###  		   ### ### ###

### ### ### INITIALIZATION ### ### ###

### ### ###  		   ### ### ###


init() {
  # app level
  FLINK_DIR="/home/myc/workspace/flink-related/flink-1.11/build-target/"
  FLINK_APP_DIR="/home/myc/workspace/flink-related/flink-testbed-org/"
  JAR=${FLINK_APP_DIR}$"target/testbed-1.0-SNAPSHOT.jar"
  ### paths configuration ###
  FLINK=$FLINK_DIR$"bin/flink"
  readonly SAVEPOINT_PATH="/home/myc/workspace/flink-related/flink-testbed-org/exp_scripts/flink_reconfig/savepoints/"
  JOB="stock.InAppStatefulStockExchange"
  EXP_NAME="stock"

  partitions=128
  parallelism=10
  runtime=100
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
    PID=`jps | grep StockGenerator | awk '{print $1}'`
    if [[ ! -z $PID ]]; then
      kill -9 ${PID}
    fi
    ${FLINK_DIR}bin/stop-cluster.sh
    echo "close finished"
    cleanEnv
}


# run applications
function runApp() {
  echo "INFO: $FLINK run -c ${JOB} ${JAR} -p2 ${parallelism} -mp2 ${partitions} &"
  rm nohup.out
  nohup $FLINK run -c ${JOB} ${JAR} -p2 ${parallelism} -mp2 ${partitions} &
}

function runGenerator() {
  echo "INFO: java -cp ${FLINK_APP_DIR}target/testbed-1.0-SNAPSHOT.jar kafkagenerator.StockGenerator > /dev/null 2>&1 &"
  java -cp ${FLINK_APP_DIR}target/testbed-1.0-SNAPSHOT.jar kafkagenerator.StockGenerator > /dev/null 2>&1 &
}

# run applications
function reconfigApp() {
  JOB_ID=$(cat nohup.out | sed -n '1p' | rev | cut -d' ' -f 1 | rev)
  JOB_ID=$(echo $JOB_ID |tr -d '\n')
  echo "INFO: running job: $JOB_ID"

  savepointPathStr=$($FLINK cancel -s $SAVEPOINT_PATH $JOB_ID)
  savepointFile=$(echo $savepointPathStr| rev | cut -d'/' -f 1 | rev)
  x=$(echo $savepointFile |tr -d '.')
  x=$(echo $x |tr -d '\n')

  rm nohup.out
  echo "INFO: RECOVER $FLINK run -d -s $SAVEPOINT_PATH$x -c ${JOB} ${JAR} -p2 ${parallelism} -mp2 ${partitions} &"
  nohup $FLINK run -d -s $SAVEPOINT_PATH$x --class $JOB $JAR  -p2 ${parallelism} -mp2 ${partitions} &
}

# run one flink demo exp, which is a word count job
run_one_exp() {
  # compute n_tuples from per task rates and parallelism
  echo "INFO: run exp stock exchange"
#  configFlink
  runFlink
  python -c 'import time; time.sleep(5)'

  runApp
  runGenerator


  python -c 'import time; time.sleep(10)'

  reconfigApp

  python -c 'import time; time.sleep(95)'

  parallelism=11
  reconfigApp

  python -c 'import time; time.sleep(100)'

  parallelism=12
  reconfigApp

  python -c 'import time; time.sleep(200)'

  parallelism=11
  reconfigApp

  SCRIPTS_RUNTIME=`expr ${runtime} - 50 + 10`
  python -c 'import time; time.sleep('"${SCRIPTS_RUNTIME}"')'
  stopFlink
}

init
run_one_exp