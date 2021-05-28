#!/bin/bash

FLINK_DIR="/path/to/Flink"
FLINK_APP_DIR="/path/to/Flink_app"

EXP_DIR="/data" # output dir

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

# configure parameters in flink bin
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


# draw figures
function analyze() {
    #python2 ${FLINK_APP_DIR}/nexmark_scripts/draw/RateAndWindowDelay.py ${EXP_NAME} ${WARMUP} ${RUNTIME}
    echo "INFO: dump to ${EXP_DIR}/raw/${EXP_NAME}"
    if [[ -d ${EXP_DIR}/raw/${EXP_NAME} ]]; then
        rm -rf ${EXP_DIR}/raw/${EXP_NAME}
    fi
    mv ${FLINK_DIR}/log ${EXP_DIR}/trisk/
    mv ${EXP_DIR}/trisk/ ${EXP_DIR}/raw/${EXP_NAME}
    mkdir ${EXP_DIR}/trisk/
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

  analyze
  stopFlink

  python -c 'import time; time.sleep(5)'
}

# initialization of the parameters
init() {
  # app level
  JAR="${FLINK_APP_DIR}/target/testbed-1.0-SNAPSHOT.jar"
  job="flinkapp.StatefulDemoLongRun"
  controller="PerformanceEvaluator"
  runtime=100
  source_p=5
#  n_tuples=15000000
  per_task_rate=6000
  parallelism=10
  key_set=10000
  per_key_state_size=1024 # byte

  # system level
  operator="Splitter FlatMap"
  reconfig_interval=10000
  reconfig_type="remap"
#  frequency=1 # deprecated
  affected_tasks=2
  repeat=1
}

# run the micro benchmarks
run_micro() {
  init
  # for repeat in 1 2 3 4 5; do # 1 2 3 4 5
  for repeat in 1; do # 1 2 3 4 5
    for reconfig_type in "remap"; do
      # parallelism
#      for parallelism in 5 10 20; do # 5 10 20
#        run_one_exp
#      done
#      parallelism=20
#
#     # arrival rate
#     for per_task_rate in 1000 2000 4000 6000 7000 8000; do # 1000 2000 4000 6000 8000 10000
#       run_one_exp
#     done
#     per_task_rate=6000

     # state size
     per_task_rate=5000
     for per_key_state_size in 1024 10240 102400; do
       run_one_exp
     done
     per_key_state_size=1024
    done
  done

#   # for repeat in 1 2 3 4 5; do # 1 2 3 4 5
#   for repeat in 1; do # 1 2 3 4 5
#     for reconfig_type in "remap" "rescale"; do
#      # number of affected tasks
#      for affected_tasks in 2 4 8 10; do # 2 4 8 10 12 14 16
#        run_one_exp
#      done
#
#      affected_tasks=2
#     done
#   done
}

## initialization for nexmark
#init_nexmark() {
#  # app level
#  JAR="${FLINK_APP_DIR}/target/testbed-1.0-SNAPSHOT.jar"
#  job="Nexmark.queries.Query2"
#  key_set=1000
#  runtime=100
#  source_p=5
#  per_task_rate=6000
#  parallelism=10
#  per_key_state_size=1024 # byte
#
#  # system level
#  operator="Splitter FlatMap"
#  reconfig_interval=10000
#  reconfig_type="remap"
##  frequency=1 # deprecated
#  affected_tasks=2
#  repeat=1
#}

## run one nexmark exp
#run_one_nexmark_exp() {
#  # compute n_tuples from per task rates and parallelism
#  n_tuples=`expr ${runtime} \* ${per_task_rate} \* ${parallelism} \/ ${source_p}`
#  EXP_NAME=trisk-${reconfig_type}-${reconfig_interval}-${runtime}-${parallelism}-${per_task_rate}-${key_set}-${per_key_state_size}-${affected_tasks}-${repeat}
#
#  echo "INFO: run exp ${EXP_NAME}"
#  configFlink
#  runFlink
#
#  python -c 'import time; time.sleep(5)'
#
#  runApp
#
#  SCRIPTS_RUNTIME=`expr ${runtime} + 10`
#  python -c 'import time; time.sleep('"${SCRIPTS_RUNTIME}"')'
#
#  analyze
#  stopFlink
#
#  python -c 'import time; time.sleep(5)'
#}
#
## run nexmark
#run_nexmark() {
#  init_nexmark
#
#  for reconfig_type in "remap" "noop"; do
#    # parallelism Q2 Q5 Q8
#    for parallelism in 5 10 20; do
#      run_one_nexmark_exp
#    done
#
#    parallelism=10
#    # arrival rate Q2 Q5 Q8
#    for per_task_rate in 1000 2000 4000 6000 8000; do
#      run_one_nexmark_exp
#    done
#
#    per_task_rate=6000
#    # affected tasks Q2 Q5 Q8
#    for affected_tasks in 2 4 6 8 10; do # 2 4 6 8 10
#      run_one_nexmark_exp
#    done
#
#    affected_tasks=2
#    # state size/ window size Q8
#    for per_key_state_size in 1024 10240 20480 40960; do
#      run_one_nexmark_exp
#    done
#  done
#}

run_micro

# dump the statistics when all exp are finished
# in the future, we will draw the intuitive figures
python ./analysis/performance_analyzer.py