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

package Nexmark.queries;

import Nexmark.sinks.DummyLatencyCountingSink;
import Nexmark.sources.keyed.KeyedBidSourceFunction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class Query5Keyed {

    private static final Logger logger  = LoggerFactory.getLogger(Query5Keyed.class);

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStateBackend(new MemoryStateBackend(100000000));


        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);

        // enable latency tracking
        env.getConfig().setLatencyTrackingInterval(5000);

        final int srcRate = params.getInt("srcRate", 100000);
        final int srcCycle = params.getInt("srcCycle", 60);
        final int srcBase = params.getInt("srcBase", 0);
        final int srcWarmUp = params.getInt("srcWarmUp", 100);

        DataStream<Tuple2<Long, Bid>> bids = env.addSource(new KeyedBidSourceFunction(srcRate, srcCycle, srcBase, srcWarmUp*1000))
                .setParallelism(params.getInt("p-bid-source", 1))
                .assignTimestampsAndWatermarks(new TimestampAssigner())
                .keyBy(0);

        // SELECT B1.auction, count(*) AS num
        // FROM Bid [RANGE 60 MINUTE SLIDE 1 MINUTE] B1
        // GROUP BY B1.auction
        DataStream<Tuple2<Long, Long>> windowed = bids.keyBy(new KeySelector<Tuple2<Long, Bid>, Long>() {
            @Override
            public Long getKey(Tuple2<Long, Bid> bid) throws Exception {
                return bid.f0;
            }
        }).timeWindow(Time.seconds(10), Time.seconds(1))
                .aggregate(new CountBids())
                .name("Sliding Window")
                .setParallelism(params.getInt("p-window", 1));


        ((SingleOutputStreamOperator<Tuple2<Long, Long>>) windowed).disableChaining();
        ((SingleOutputStreamOperator<Tuple2<Long, Long>>) windowed).setMaxParallelism(params.getInt("mp2", 64));
        ((SingleOutputStreamOperator<Tuple2<Long, Long>>) windowed).setParallelism(params.getInt("p2",  1));
        ((SingleOutputStreamOperator<Tuple2<Long, Long>>) windowed).name("window");

        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        windowed.transform("DummyLatencySink", objectTypeInfo, new DummyLatencyCountingSink<>(logger))
                .setParallelism(params.getInt("p-window", 1));

        // execute program
        env.execute("Nexmark Query5");
    }

    private static final class TimestampAssigner implements AssignerWithPeriodicWatermarks<Tuple2<Long, Bid>> {
        private long maxTimestamp = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(maxTimestamp);
        }

        @Override
        public long extractTimestamp(Tuple2<Long, Bid> element, long previousElementTimestamp) {
            maxTimestamp = Math.max(maxTimestamp, element.f1.dateTime);
            return element.f1.dateTime;
        }
    }

    private static final class CountBids implements AggregateFunction<Tuple2<Long, Bid>, Long, Tuple2<Long, Long>> {

        private long auction = 0L;

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<Long, Bid> value, Long accumulator) {
            auction = value.f1.auction;
            return accumulator + 1;
        }

        @Override
        public Tuple2<Long, Long> getResult(Long accumulator) {
            return new Tuple2<>(auction, accumulator);
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }
}
