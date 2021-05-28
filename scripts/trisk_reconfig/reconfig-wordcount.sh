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
    sed 's/^\(\s*trisk.reconfig.interval\s*:\s*\).*/\1'"$reconfig_interval"'/' tmp1 > tmp2
    sed 's/^\(\s*trisk.reconfig.affected_tasks\s*:\s*\).*/\1'"$affected_tasks"'/' tmp2 > tmp3
    sed 's/^\(\s*trisk.controller\s*:\s*\).*/\1'"$controller"'/' tmp3 > tmp4
    sed 's/^\(\s*trisk.reconfig.type\s*:\s*\).*/\1'"$reconfig_type"'/' tmp4 > ${FLINK_DIR}/conf/flink-conf.yaml
    rm tmp1 tmp2 tmp3 tmp4
}

# run applications
function runApp() {
  echo "INFO: ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -runtime ${runtime} -nTuples ${n_tuples}  \-p1 ${source_p} -p2 ${parallelism} \
    -nKeys ${key_set} -perKeySize ${per_key_state_size} &"
  ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -runtime ${runtime} -nTuples ${n_tuples}  \-p1 ${source_p} -p2 ${parallelism} \
    -nKeys ${key_set} -perKeySize ${per_key_state_size} &
}


# run one flink demo exp, which is a word count job
run_one_exp() {
  # compute n_tuples from per task rates and parallelism
  n_tuples=`expr ${runtime} \* ${per_task_rate} \* ${parallelism} \/ ${source_p}`
  EXP_NAME=trisk-${reconfig_type}-${reconfig_interval}-${runtime}-${parallelism}-${per_task_rate}-${key_set}-${per_key_state_size}-${affected_tasks}-${repeat}

  echo "INFO: run exp ${EXP_NAME}"
  configFlink
  runFlink

  python -c 'import time; time.sleep(5)'

  runApp

  SCRIPTS_RUNTIME=`expr ${runtime} + 10`
  python -c 'import time; time.sleep('"${SCRIPTS_RUNTIME}"')'

#  analyze
  stopFlink

  python -c 'import time; time.sleep(5)'
}

# initialization of the parameters
init() {
  # app level
  JAR="${FLINK_APP_DIR}/target/testbed-1.0-SNAPSHOT.jar"
  job="flinkapp.StatefulDemoLongRun"
  controller="DummyController"
  runtime=200
  source_p=1
#  n_tuples=15000000
  per_task_rate=5000
  parallelism=10
  key_set=1000
  per_key_state_size=40960 # byte
  operator="Splitter FlatMap"

  # these configs will be never used in DummyController
  reconfig_interval=10000
  reconfig_type="remap"
#  frequency=1 # deprecated
  affected_tasks=2
  repeat=1
}

# run the micro benchmarks
run() {
  init
  run_one_exp
}

run
