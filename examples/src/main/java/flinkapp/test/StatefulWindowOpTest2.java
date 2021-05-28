package flinkapp.test;

import Nexmark.sinks.DummySink;
import flinkapp.test.utils.RescaleActionDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.HashMap;
import java.util.Map;

public class StatefulWindowOpTest2 {

    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Integer>> source = env.addSource(new WordSource(
                params.getInt("runtime", 30),
                params.getInt("nTuples", 10000),
                params.getInt("nKeys", 30000)
        ));
        DataStream<Tuple2<String, Integer>> counts = source
                .keyBy(0)
                .timeWindow(Time.seconds(1))
                .max(0)
                .name("map" + new RescaleActionDescriptor()
                        .thenScaleOut(4)
                        .toString())
                .uid("window")
                .setParallelism(params.getInt("p2", 3));

        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        counts.transform("Sink", objectTypeInfo,
                new DummySink<>())
                .uid("dummy-sink")
                .setParallelism(params.getInt("p3", 1));

        env.execute("window scale out 3 to 4");
    }

    private static class WordSource implements SourceFunction<Tuple2<String, Integer>>, CheckpointedFunction {

        private int count = 0;
        private volatile boolean isRunning = true;

        private transient ListState<Integer> checkpointedCount;

        private int executionTime;
        private int nTuples;
        private int nKeys;
        private int rate;
        private Map<String, Integer> keyCount = new HashMap<>();

        WordSource(int executionTime, int nTuples, int nKeys) {
            this.executionTime = executionTime;
            this.nTuples = nTuples;
            this.nKeys = nKeys;
            this.rate = nTuples / executionTime;
            System.out.println("runtime: " + executionTime
                    + ", nTuples: " + nTuples
                    + ", nKeys: " + nKeys
                    + ", rate: " + rate);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            //??? I guess this list only have one element
            this.checkpointedCount.clear();
            this.checkpointedCount.add(count);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            this.checkpointedCount = context
                    .getOperatorStateStore()
                    .getListState(new ListStateDescriptor<>("checkpointedCount", Integer.class));

            if (context.isRestored()) {
                //???
                for (Integer count : this.checkpointedCount.get()) {
                    this.count = count;
                }
            }
        }

        @Override
        public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
            while (isRunning && count < nTuples) {
                if (count % rate == 0) {
                    Thread.sleep(1000);
                }
                synchronized (ctx.getCheckpointLock()) {
                    String key = getChar(count);
                    int curCount = keyCount.getOrDefault(key, 0) + 1;
                    keyCount.put(key, curCount);
                    System.out.println("sent: " + key + " : " + curCount + " total: " + count);
                    ctx.collect(Tuple2.of(key, 1));

                    count++;
                }
            }
        }

        private String getChar(int cur) {
            return "A" + (cur % nKeys);
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}


