package flinkapp;

import Nexmark.sources.Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.HashMap;
import java.util.Map;

import static common.Util.delay;

/**
 * Test Job for the most fundemental functionalities,
 * 1. single scaling, mutliple scaling.
 * 2. different arrival rates, testing scaling on under-loaded job and over-loaded job.
 * 3. different key distributions, whether the final key count is consistent.
 */

public class StatefulDemoLongRun {

    private static final int MAX = 1000000 * 10;
    //    private static final int MAX = 1000;
    private static final int NUM_LETTERS = 26;

    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.enableCheckpointing(1000);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // set up the execution environment
        env.setStateBackend(new MemoryStateBackend(1073741824));

        int perKeyStateSize = params.getInt("perKeySize", 1024);

        DataStreamSource<Tuple2<String, String>> source = env.addSource(new MySource(
                params.getInt("runtime", 10),
                params.getInt("nTuples", 10000),
                params.getInt("nKeys", 1000)
        )).setParallelism(params.getInt("p1", 1));
        DataStream<String> counts = source
                .keyBy(0)
                .map(new MyStatefulMap(perKeyStateSize))
                .disableChaining()
//            .filter(input -> {
//                return Integer.parseInt(input.split(" ")[1]) >= MAX;
//            })
                .name("Splitter FlatMap")
                .uid("flatmap")
                .setParallelism(params.getInt("p2", 3));

//        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
//        counts.transform("Sink", objectTypeInfo,
//                new DummySink<>())
//                .uid("dummy-sink")
//                .setParallelism(params.getInt("p3", 1));
//        counts.print();
        env.execute();
//        System.out.println(env.getExecutionPlan());
    }

    private static class MyStatefulMap extends RichMapFunction<Tuple2<String, String>, String> {

        private transient MapState<String, String> countMap;

        private int count = 0;

        private final int perKeyStateSize;

        private final String payload;

        public MyStatefulMap(int perKeyStateSize) {
            this.perKeyStateSize = perKeyStateSize;
            this.payload = StringUtils.repeat("A", perKeyStateSize);
        }

        @Override
        public String map(Tuple2<String, String> input) throws Exception {
            delay();

            String s = input.f0;

            Long cur = 1L;
            countMap.put(s, payload);

//            Long cur = countMap.get(s);
//            cur = (cur == null) ? 1 : cur + 1;
//            countMap.put(s, cur);

//            count++;
//            System.out.println("counted: " + s + " : " + cur);

//            System.out.println("ts: " + Long.parseLong(input.f1) + " endToEnd latency: " + (System.currentTimeMillis() - Long.parseLong(input.f1)));

            return String.format("%s %d", s, cur);
        }

        @Override
        public void open(Configuration config) {
            MapStateDescriptor<String, String> descriptor =
                    new MapStateDescriptor<>("word-count", String.class, String.class);

            countMap = getRuntimeContext().getMapState(descriptor);
        }
    }

    public static class IncreaseCommunicationOverheadMap extends MyStatefulMap{

        private final int numRepeat;

        public IncreaseCommunicationOverheadMap(int numRepeat){
            super(1024);
            this.numRepeat = numRepeat;
        }

        @Override
        public String map(Tuple2<String, String> input) throws Exception {
            String result = super.map(input);
            StringBuilder sb = new StringBuilder("Just to Increase Communication Overhead:");
            for(int i=0;i<numRepeat;i++){
                sb.append(result).append(' ');
            }
            return sb.toString();
        }
    }

    public static class IncreaseComputationOverheadMap extends MyStatefulMap{
        // in ms
        private final int timeToWait;

        public IncreaseComputationOverheadMap(int timeToWait){
            super(1024);
            this.timeToWait = timeToWait;
        }

        @Override
        public String map(Tuple2<String, String> input) throws Exception {
            long start = System.nanoTime();
            while(System.nanoTime() - start < 1000*timeToWait);
            return super.map(input);
        }
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
//            while (isRunning && count < nTuples) {
//                if (count % rate  == 0) {
//                    Thread.sleep(1000);
//                }
//                synchronized (ctx.getCheckpointLock()) {
//                    String key = getChar(count);
//                    int curCount = keyCount.getOrDefault(key, 0)+1;
//                    keyCount.put(key, curCount);
//                    System.out.println("sent: " + key + " : " + curCount + " total: " + count);
//                    ctx.collect(Tuple2.of(key, key));
//
//                    count++;
//                }
//            }

            long emitStartTime = System.currentTimeMillis();

            while (isRunning && count < nTuples) {
//                System.out.println("++++++interval: " + (System.currentTimeMillis() - emitStartTime));
                emitStartTime = System.currentTimeMillis();
                for (int i = 0; i < rate / 20; i++) {
//                    synchronized (ctx.getCheckpointLock()) {
                    String key = getChar(count);
                    int curCount = keyCount.getOrDefault(key, 0)+1;
                    keyCount.put(key, curCount);
//                    System.out.println("sent: " + key + " : " + curCount + " total: " + count);
//                    ctx.collect(Tuple2.of(key, key));
                    ctx.collect(Tuple2.of(key, String.valueOf(System.currentTimeMillis())));

                    count++;
//                    }
                }

                // Sleep for the rest of timeslice if needed
                Util.pause(emitStartTime);
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
