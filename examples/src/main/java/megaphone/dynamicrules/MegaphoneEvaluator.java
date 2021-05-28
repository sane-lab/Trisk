/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package megaphone.dynamicrules;

import Nexmark.sources.Util;
import megaphone.config.Config;
import megaphone.dynamicrules.functions.MapProcessorFunction;
import megaphone.dynamicrules.functions.MapRouterFunction;
import megaphone.dynamicrules.functions.ProcessorFunction;
import megaphone.dynamicrules.functions.RouterFunction;
import megaphone.dynamicrules.sinks.AlertsSink;
import megaphone.dynamicrules.sources.ControlMessageSource;
import lombok.extern.slf4j.Slf4j;
import megaphone.config.Parameters;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MegaphoneEvaluator {

  private Config config;

  private final Map<String, String> globalState = new HashMap<>();


  MegaphoneEvaluator(Config config) {
    this.config = config;
  }

  public void run() throws Exception {

    ControlMessageSource.Type rulesSourceType = getControlMessageSourceType();

    boolean isLocal = config.get(Parameters.LOCAL_EXECUTION);
    boolean enableCheckpoints = config.get(Parameters.ENABLE_CHECKPOINTS);
    int checkpointsInterval = config.get(Parameters.CHECKPOINT_INTERVAL);
    int minPauseBtwnCheckpoints = config.get(Parameters.CHECKPOINT_INTERVAL);

    // Environment setup
    StreamExecutionEnvironment env = configureStreamExecutionEnvironment(rulesSourceType, isLocal);

    if (enableCheckpoints) {
      env.enableCheckpointing(checkpointsInterval);
      env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPauseBtwnCheckpoints);
    }

    // Streams setup
    DataStream<String> controlMessageUpdateStream = getControlMessageUpdateStream(env);
    DataStream<Tuple2<String, String>> words = getWordsStream(env);

    BroadcastStream<String> rulesStream = controlMessageUpdateStream.broadcast(Descriptors.rulesDescriptor);

    // Processing pipeline setup
    // TODO: use view number to control the version of the config, such that to keep consistency and correctness.
    DataStream<Alert> alerts =
        words
//            .connect(rulesStream)
//            .process(new RouterFunction())
            .flatMap(new MapRouterFunction())
            .setParallelism(1)
            .uid("RouterFunction")
            .name("Dynamic Partitioning Function")
            .keyBy((keyed) -> keyed.getKey())
//            .connect(rulesStream)
//            .process(new ProcessorFunction())
            .map(new MapProcessorFunction())
            .setParallelism(10)
            .uid("ProcessorFunction")
            .name("Dynamic ControlMessage Evaluation Function");

//    alerts.print().name("Alert STDOUT Sink");

    DataStream<String> alertsJson = AlertsSink.alertsStreamToJson(alerts);

//    alertsJson
//        .addSink(AlertsSink.createAlertsSink(config))
//        .setParallelism(1)
//        .name("Alerts JSON Sink");

    env.execute("Fraud Detection Engine");
  }

  private DataStream<Tuple2<String, String>> getWordsStream(StreamExecutionEnvironment env) {
    // Data stream setup
//    SourceFunction<Tuple2<String, String>> wordsSource = new MySource(50, 12800000, 128);
    SourceFunction<Tuple2<String, String>> wordsSource = new MySource(200, 10000000, 1000);
    int sourceParallelism = config.get(Parameters.SOURCE_PARALLELISM);
    return env.addSource(wordsSource)
        .name("Transactions Source")
        .setParallelism(1)
        .keyBy(0);
  }

  private DataStream<String> getControlMessageUpdateStream(StreamExecutionEnvironment env) throws IOException {

    ControlMessageSource.Type rulesSourceEnumType = getControlMessageSourceType();

    SourceFunction<String> controlMessageSource = ControlMessageSource.createControlMessageSource(config);
    return env.addSource(controlMessageSource).name(rulesSourceEnumType.getName()).setParallelism(1);
  }

  private ControlMessageSource.Type getControlMessageSourceType() {
    String controlMessageSource = config.get(Parameters.RULES_SOURCE);
    return ControlMessageSource.Type.valueOf(controlMessageSource.toUpperCase());
  }

  private StreamExecutionEnvironment configureStreamExecutionEnvironment(
          ControlMessageSource.Type rulesSourceEnumType, boolean isLocal) {
    Configuration flinkConfig = new Configuration();
    flinkConfig.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);

    StreamExecutionEnvironment env =
        isLocal
            ? StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig)
            : StreamExecutionEnvironment.getExecutionEnvironment();

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getCheckpointConfig().setCheckpointInterval(config.get(Parameters.CHECKPOINT_INTERVAL));
    env.getCheckpointConfig()
        .setMinPauseBetweenCheckpoints(config.get(Parameters.MIN_PAUSE_BETWEEN_CHECKPOINTS));

    configureRestartStrategy(env, rulesSourceEnumType);
    return env;
  }

  private static class SimpleBoundedOutOfOrdernessTimestampExtractor<T extends Transaction>
      extends BoundedOutOfOrdernessTimestampExtractor<T> {

    public SimpleBoundedOutOfOrdernessTimestampExtractor(int outOfOrderdnessMillis) {
      super(Time.of(outOfOrderdnessMillis, TimeUnit.MILLISECONDS));
    }

    @Override
    public long extractTimestamp(T element) {
      return element.getEventTime();
    }
  }

  private void configureRestartStrategy(
      StreamExecutionEnvironment env, ControlMessageSource.Type rulesSourceEnumType) {
    switch (rulesSourceEnumType) {
      case SOCKET:
        env.setRestartStrategy(
            RestartStrategies.fixedDelayRestart(
                10, org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)));
        break;
      case KAFKA:
        // Default - unlimited restart strategy.
        //        env.setRestartStrategy(RestartStrategies.noRestart());
    }
  }

  public static class Descriptors {
    public static final MapStateDescriptor<String, Integer> rulesDescriptor =
            new MapStateDescriptor<>("rules", String.class, Integer.class);
  }

  private static class MySource extends RichParallelSourceFunction<Tuple2<String, String>> implements CheckpointedFunction {

    private int count = 0;
    private volatile boolean isRunning = true;

    private transient ListState<Integer> checkpointedCount;

    private int runtime;
    private int nTuples;
    private int nKeys;
    private int rate;
    private Map<String, Integer> keyCount = new HashMap<>();
    private Random r =	new Random();

    MySource(int runtime, int nTuples, int nKeys) {
      this.runtime = runtime;
      this.nTuples = nTuples;
      this.nKeys = nKeys;
      this.rate = nTuples / runtime;
      System.out.println("runtime: " + runtime
              + ", nTuples: " + nTuples
              + ", nKeys: " + nKeys
              + ", rate: " + rate);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
      this.checkpointedCount.clear();
      this.checkpointedCount.add(count);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
      this.checkpointedCount = context
              .getOperatorStateStore()
              .getListState(new ListStateDescriptor<>("checkpointedCount", Integer.class));

      if (context.isRestored()) {
        for (Integer count : this.checkpointedCount.get()) {
          this.count = count;
        }
      }
    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
      long emitStartTime;

      while (isRunning && count < nTuples) {
        emitStartTime = System.currentTimeMillis();
        for (int i = 0; i < rate / 20; i++) {
          String key = getChar(count);
//          String key = getKey();
          int curCount = keyCount.getOrDefault(key, 0)+1;
          keyCount.put(key, curCount);
          ctx.collect(Tuple2.of(key, String.valueOf(System.currentTimeMillis())));
          count++;
        }
        // Sleep for the rest of timeslice if needed
        Util.pause(emitStartTime);
      }
    }

    private String getChar(int cur) {
      return "A" + (cur % nKeys);
//      return "A" + (r.nextInt(nKeys-1)%nKeys);
    }

    private static String getKey() {
      return String.valueOf(Math.random());
    }

    public int getRandomNumber(int min, int max) {
      return (int) ((Math.random() * (max - min)) + min);
    }

    @Override
    public void cancel() {
      isRunning = false;
    }
  }
}
