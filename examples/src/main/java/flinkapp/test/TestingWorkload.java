package flinkapp.test;

import Nexmark.sources.AuctionSourceFunction;
import Nexmark.sources.PersonSourceFunction;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * This class should not be edited. This is a test work load for TestingControlPolicy.
 * To use this workload, copy it in another module and package it (may need to modify the package name).
 * <p>
 * <p>
 * <p>
 * Test Job for the most fundemental functionalities,
 * 1. single scaling, mutliple scaling.
 * 2. different arrival rates, testing scaling on under-loaded job and over-loaded job.
 * 3. different key distributions, whether the final key count is consistent.
 */
public class TestingWorkload {

    private static final int MAX = 1000000 * 10;
    //    private static final int MAX = 1000;
    private static final int NUM_LETTERS = 26;

    private static void simpleTest(StreamExecutionEnvironment env, ParameterTool params) {

        DataStreamSource<Tuple2<String, Long>> source = env.addSource(new MySource(
                params.getInt("runtime", 50),
                params.getInt("nTuples", 10000),
                params.getInt("nKeys", 1000)
        ));
        DataStream<Tuple2<String, Long>> mapStream = source
                .name("source")
                .disableChaining()
                .keyBy(0)
                .map(new MapFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Tuple2<String, Long> stringLongTuple2) throws Exception {
                        return stringLongTuple2;
                    }
                })
                .name("fake source")
                .setParallelism(2)
                .keyBy(0)
                .map(new MyStatefulMap("fake source"))
                .name("Splitter FlatMap")
                .setParallelism(2);

        DataStream<Tuple2<String, Long>> counts = mapStream
                .keyBy(0)
                .filter(input -> {
                    System.out.println("filter:" + input);
                    return true;
                })
                .name("filter")
                .uid("filter")
                .setParallelism(params.getInt("p2", 2));

        counts.keyBy(0).transform(
                "Count Sink", new GenericTypeInfo<>(Object.class), new DummyNameSink<>("count sink"))
                .uid("dummy-count-sink")
                .setParallelism(params.getInt("p3", 1));
        // second stream
        source.disableChaining()
                .keyBy(0)
                .map(new MyStatefulMap("real source"))
                .name("near source Flatmap")
                .setParallelism(2);
        // third stream
        source.disableChaining()
                .keyBy(0)
                .map(new MapFunction<Tuple2<String, Long>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Tuple2<String, Long> stringLongTuple2) throws Exception {
                        return Tuple2.of(stringLongTuple2.f0, 1);
                    }
                })
                .name("source stateless map")
                .setParallelism(params.getInt("p2", 2))
                .keyBy(0)
                .countWindow(10)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
                        return Tuple2.of(stringIntegerTuple2.f0 + " " + t1.f0, stringIntegerTuple2.f1 + t1.f1);
                    }
                })
                .setParallelism(2)
                .name("counting window reduce")
                .transform("Filter Sink", new GenericTypeInfo<>(Object.class), new DummyNameSink<>("Filter sink"))
                .setParallelism(params.getInt("p3", 1));
    }


    private static void windowJoinTest(StreamExecutionEnvironment env, ParameterTool params) throws Exception {

        final int auctionSrcRate = params.getInt("auction-srcRate", 1000);
        final int auctionSrcCycle = params.getInt("auction-srcCycle", 10);
        final int auctionSrcBase = params.getInt("auction-srcBase", 0);

        final int personSrcRate = params.getInt("person-srcRate", 1000);
        final int personSrcCycle = params.getInt("person-srcCycle", 10);
        final int personSrcBase = params.getInt("person-srcBase", 0);


        DataStream<Auction> auctions = env.addSource(new AuctionSourceFunction(auctionSrcRate, auctionSrcCycle, auctionSrcBase))
                .name("Custom Source: Auctions")
                .setParallelism(params.getInt("p-auction-source", 1));

        DataStream<Person> persons = env.addSource(new PersonSourceFunction(personSrcRate, personSrcCycle, personSrcBase))
                .name("Custom Source: Persons")
                .setParallelism(params.getInt("p-person-source", 1));

        // SELECT Rstream(P.id, P.name, A.reserve)
        // FROM Person [RANGE 1 HOUR] P, Auction [RANGE 1 HOUR] A
        // WHERE P.id = A.seller;
        DataStream<Tuple3<Long, String, Long>> joined = persons
                .join(auctions)
                .where((KeySelector<Person, Long>) p -> p.id)
                .equalTo((KeySelector<Auction, Long>) a -> a.seller)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(1)))
                .apply(
                        new FlatJoinFunction<Person, Auction, Tuple3<Long, String, Long>>() {
                            @Override
                            public void join(Person person, Auction auction, Collector<Tuple3<Long, String, Long>> collector) throws Exception {
                                collector.collect(new Tuple3<>(person.id, person.name, auction.reserve));
                            }
                        }
                );
        ((SingleOutputStreamOperator<Tuple3<Long, String, Long>>) joined)
                .disableChaining()
                .name("join1")
                .setMaxParallelism(params.getInt("mp2", 128))
                .setParallelism(params.getInt("p2", 2));

        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        joined.transform("DummySink", objectTypeInfo, new DummyNameSink<>("join sink"))
                .uid("dummy-sink-join1")
                .setParallelism(params.getInt("p-window", 1));
    }

    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.getConfig().setAutoWatermarkInterval(1000);
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.disableOperatorChaining();
        env.setParallelism(params.getInt("p-window", 1));

        simpleTest(env, params);
        windowJoinTest(env, params);

        System.out.println(env.getExecutionPlan());
        env.execute();
    }

    private static class MyStatefulMap extends RichMapFunction<Tuple2<String, Long>, Tuple2<String, Long>> {

        private transient MapState<String, Long> countMap;
        private final String name;
        private int count = 0;

        private MyStatefulMap(String name) {
            this.name = name;
        }

        @Override
        public Tuple2<String, Long> map(Tuple2<String, Long> input) throws Exception {
            long start = System.nanoTime();
            // loop 0.01 ms
            while (System.nanoTime() - start < 10000) ;

            String s = input.f0;

            Long cur = countMap.get(s);
            cur = (cur == null) ? 1 : cur + 1;
            countMap.put(s, cur);

            count++;
            System.out.println(name + " counted: " + s + " : " + cur);
            if (!input.f1.equals(cur)) {
                System.err.println("why are this not equal with input oracle:" + input);
            }
            return Tuple2.of(input.f0, cur);
        }

        @Override
        public void open(Configuration config) {
            MapStateDescriptor<String, Long> descriptor =
                    new MapStateDescriptor<>("word-count", String.class, Long.class);

            countMap = getRuntimeContext().getMapState(descriptor);
        }
    }

    private static class MySource implements SourceFunction<Tuple2<String, Long>>, CheckpointedFunction {

        private int count = 0;
        private volatile boolean isRunning = true;

        private transient ListState<Integer> checkpointedCount;

        private int nTuples;
        private int nKeys;
        private int rate;
        private Map<String, Integer> keyCount = new HashMap<>();

        MySource(int runtime, int nTuples, int nKeys) {
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
        public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
            while (isRunning && count < nTuples) {
                if (count % rate == 0) {
                    Thread.sleep(1000);
                }
                synchronized (ctx.getCheckpointLock()) {
                    String key = getChar(count);
                    int curCount = keyCount.getOrDefault(key, 0) + 1;
                    keyCount.put(key, curCount);
                    System.out.println("sent: " + key + " : " + curCount + " total: " + count);
                    ctx.collect(Tuple2.of(key, (long) curCount));

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

    /**
     * A Sink that drops all data
     */
    public static class DummyNameSink<T> extends StreamSink<T> {

        public DummyNameSink(String name) {
            super(new SinkFunction<T>() {
                int received = 0;

                @Override
                public void invoke(T value, Context ctx) throws Exception {
                    received++;
                    System.out.println(name + " received " + received + "th record:" + value);
                }
            });
        }
    }
}


