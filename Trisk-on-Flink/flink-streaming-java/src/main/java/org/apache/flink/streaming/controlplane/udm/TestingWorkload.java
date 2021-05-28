package org.apache.flink.streaming.controlplane.udm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSink;

import java.util.HashMap;
import java.util.Map;

/**
 * This class should not be edited. This is a test work load for TestingController.
 * To use this workload, copy it in another module and package it (may need to modify the package name).
 *
 *
 *
 * Test Job for the most fundemental functionalities,
 * 1. single scaling, mutliple scaling.
 * 2. different arrival rates, testing scaling on under-loaded job and over-loaded job.
 * 3. different key distributions, whether the final key count is consistent.
 */
public class TestingWorkload {

	private static final int MAX = 1000000 * 10;
	//    private static final int MAX = 1000;
	private static final int NUM_LETTERS = 26;

	public static void main(String[] args) throws Exception {
		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.enableCheckpointing(1000);
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

		DataStreamSource<Tuple2<String, Long>> source = env.addSource(new MySource(
			params.getInt("runtime", 10),
			params.getInt("nTuples", 10000),
			params.getInt("nKeys", 1000)
		));
		DataStream<String> mapStream = source
			.disableChaining()
			.map(new MapFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
				@Override
				public Tuple2<String, Long> map(Tuple2<String, Long> stringLongTuple2) throws Exception {
					return stringLongTuple2;
				}
			})
			.name("source")
			.setParallelism(1)
			.keyBy(0)
			.map(new MyStatefulMap())
			.name("Splitter Flatmap")
			.setParallelism(2);

		DataStream<String> counts = mapStream
			.filter(input -> true)
			.name("filter")
			.uid("filter")
			.setParallelism(params.getInt("p2", 2));

		counts.transform("Count Sink", new GenericTypeInfo<>(Object.class),
			new DummyNameSink<>("count sink"))
			.uid("dummy-count-sink")
			.setParallelism(params.getInt("p3", 1));

		System.out.println(env.getExecutionPlan());
		env.execute();
	}

	private static class MyStatefulMap extends RichMapFunction<Tuple2<String, Long>, String> {

		private transient MapState<String, Long> countMap;

		private int count = 0;

		@Override
		public String map(Tuple2<String, Long> input) throws Exception {
			long start = System.nanoTime();
			// loop 0.01 ms
			while (System.nanoTime() - start < 10000) ;

			String s = input.f0;

			Long cur = countMap.get(s);
			cur = (cur == null) ? 1 : cur + 1;
			countMap.put(s, cur);

			count++;
			System.out.println("counted: " + s + " : " + cur);
			if (!input.f1.equals(cur)) {
				System.out.println("why are this not equal with input oracle:" + input);
			}
			return String.format("%s %d", s, cur);
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
					received ++;
					System.out.println(name + " received " + received + "th record:" + value);
				}
			});
		}
	}
}


