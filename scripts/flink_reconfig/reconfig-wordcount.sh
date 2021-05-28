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
  JOB="flinkapp.KafkaStatefulDemoLongRun"

  runtime=200
  source_p=1
#  n_tuples=15000000
  per_task_rate=5000
  parallelism=10
  key_set=1000
  per_key_state_size=40960 # byte

  # system level
  operator="Splitter FlatMap"
  reconfig_interval=10000
  reconfig_type="remap"
#  frequency=1 # deprecated
  affected_tasks=2
  repeat=1
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
  echo "INFO: $FLINK run -c ${JOB} ${JAR} \
    -runtime ${runtime} -nTuples ${n_tuples}  \-p1 ${source_p} -p2 ${parallelism} \
    -nKeys ${key_set} -perKeySize ${per_key_state_size} &"
  rm nohup.out
  nohup $FLINK run -c ${JOB} ${JAR} \
    -runtime ${runtime} -nTuples ${n_tuples}  \-p1 ${source_p} -p2 ${parallelism} \
    -nKeys ${key_set} -perKeySize ${per_key_state_size} &
}

function runGenerator() {
  echo "INFO: java -cp ${FLINK_APP_DIR}target/testbed-1.0-SNAPSHOT.jar kafkagenerator.WCGenerator \
    -runtime ${runtime} -nTuples ${n_tuples} -nKeys ${key_set} > /dev/null 2>&1 &"

  java -cp ${FLINK_APP_DIR}target/testbed-1.0-SNAPSHOT.jar kafkagenerator.WCGenerator \
    -runtime ${runtime} -n                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     Tuples ${n_tuples} -nKeys ${key_set} > /dev/null 2>&1 &
}

# run applications
function reconfigApp() {
  echo "update start"
  start=$SECONDS
  JOB_ID=$(cat nohup.out | sed -n '1p' | rev | cut -d' ' -f 1 | rev)
  JOB_ID=$(echo $JOB_ID |tr -d '\n')
  echo "INFO: running job: $JOB_ID"

  savepointPathStr=$($FLINK cancel -s $SAVEPOINT_PATH $JOB_ID)
  savepointFile=$(echo $savepointPathStr| rev | cut -d'/' -f 1 | rev)
  x=$(echo $savepointFile |tr -d '.')
  x=$(echo $x |tr -d '\n')

  rm nohup.out
  echo "INFO: RECOVER $FLINK run -d -s $SAVEPOINT_PATH$x -c ${JOB} ${JAR} \
      -runtime ${runtime} -nTuples ${n_tuples}  \-p1 ${source_p} -p2 ${parallelism} \
      -nKeys ${key_set} -perKeySize ${per_key_state_size} &"
  nohup $FLINK run -d -s $SAVEPOINT_PATH$x --class $JOB $JAR \
      -runtime ${runtime} -nTuples ${n_tuples}  \
      -p1 ${source_p} -p2 ${parallelism} \
      -nKeys ${key_set} -perKeySize ${per_key_state_size} &

  duration=$(( SECONDS - start ))
  echo "++++++ completion time: $duation"
}

# run one flink demo exp, which is a word count job
run_one_exp() {
  # compute n_tuples from per task rates and parallelism
  n_tuples=`expr ${runtime} \* ${per_task_rate} \* ${parallelism} \/ ${source_p}`
  EXP_NAME=trisk-${reconfig_type}-${reconfig_interval}-${runtime}-${parallelism}-${per_task_rate}-${key_set}-${per_key_state_size}-${affected_tasks}-${repeat}

  echo "INFO: run exp ${EXP_NAME}"
#  configFlink
  runFlink
  python -c 'import time; time.sleep(5)'

  runApp
  runGenerator

  python -c 'import time; time.sleep(55)'

  reconfigApp

  SCRIPTS_RUNTIME=`expr ${runtime} - 55 + 10`
  python -c 'import time; time.sleep('"${SCRIPTS_RUNTIME}"')'
  stopFlink
}

init
per_key_state_size=40960
run_one_exp