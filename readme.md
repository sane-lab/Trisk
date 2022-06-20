# Trisk
Trisk is a control plane to provide reconfigurations with versatility, efficiency and usability.

Trisk stores its abstraction at `TriskAbstractionImpl.java`, in which Trisk also encapsulates APIs to apply primitive operations.

Trisk controllers are implemented in `XXController.java`, and all currently supported controllers can be found in `StreamManager.java`.

## Prerequisite

1. Python3
2. Zookeeper
3. Kafka
4. Java 1.8

## Code architecture

The source code of `Trisk` has been placed into `Flink`, because Flink has network stack for us to achieve RPC among our components.

The main source code entrypoint is in `flink-streaming-java/controlplane`.

To explore our source code, you can try to start from `StreamManager.java`, which is the main component to connect other components.

## How to use?

### Run an example

1. Compile `Trisk-on-Flink` with : `mvn clean install -DskipTests -Dcheckstyle.skip -Drat.skip=true`.
2. Compile `examples` with: `mvn clean package`.
3. Try Trisk with the following command: cd `Trisk-on-Flink/build-target`  and start a standalone cluster: `./bin/start-cluster.sh`.
4. Launch an example `StatefulDemo`  in  examples folder: `./bin/flink run -c flinkapp.StatefulDemo examples/target/testbed-1.0-SNAPSHOT.jar`

We have placed Trisk into the Flink, and uses Flink configuration tools to configure the parameters of Tirsk. There are some configurations you can try in `flink-conf.yaml` to use the `Trisk-on-Flink`:

| Parameter                     | Default              | Description                                                  |
| ----------------------------- | -------------------- | ------------------------------------------------------------ |
| trisk.controller              | PerformanceEvaluator | the performance controller tries to run a reconfiguration per 10s on an operator |
| trisk.reconfig.operator.name  | Splitter FlatMap     | the operator to reconfigure                                  |
| trisk.reconfig.interval       | 10000                | interval between two reconfigurations (ms)                   |
| trisk.reconfig.affected_tasks | 2                    | affected tasks for a reconfiguration (this parameter has no effect on change of logic) |
| trisk.exp.dir                 | /data                | the folder to output metrics measured during the execution.  |

### Design and run a controller

To design a controller, we have two ways as illustrated in paper.

1. create a `XXcontroller.java`  in `udm` folder of Trisk project, and compile the `Trisk-on-Flink` to have a try.
2. submit your `XXController.java` via a restful API, which is listening on port `8520`. For more details, please refer to our scripts to submit the source code: 

## Run scripts for experiments

In this project, we have mainly run experiments on three systems as illustrated in our paper:

1. Trisk experiment
2. Flink experiment
3. Megaphone experiment

We have placed our scripts to run the experiments in `scripts` folder, in which there mainly four sub-folders, `flink_reconfig`, `megaphone_reconfig`, and `trisk_reconfig` contains scripts to run the corresponding experiments of each system.

`analysis` contains the analysis scripts to process raw data and draw figures shown in our paper.

Each experiment script requires some configurations:

| Variable      | Default              | Description                        |
| ------------- | -------------------- | ---------------------------------- |
| FLINK_DIR     | "/path/to/Flink"     | path to compiled  `Trisk-on-Flink` |
| FLINK_APP_DIR | "/path/to/Flink_app" | path to the compiled `example`     |
| EXP_DIR       | "/data"              | path for the raw data output       |

For scripts to draw figures:

| Variable      | Default         | Description            |
| ------------- | --------------- | ---------------------- |
| FIGURE_FOLDER | '/data/results' | path to output figures |
| FILE_FOLER    | '/data/raw'     | path to read raw data  |


