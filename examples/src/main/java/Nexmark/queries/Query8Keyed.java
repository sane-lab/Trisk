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
import Nexmark.sources.keyed.KeyedAuctionSourceFunction;
import Nexmark.sources.keyed.KeyedPersonSourceFunction;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class Query8Keyed {

    private static final Logger logger  = LoggerFactory.getLogger(Query8Keyed.class);

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new FsStateBackend("file:///home/myc/workspace/flink-related/states"));
//        env.setStateBackend(new FsStateBackend("hdfs://camel:9000/flink/checkpoints"));

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);

        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // enable latency tracking
        //env.getConfig().setLatencyTrackingInterval(5000);

        final int auctionSrcRate = params.getInt("auction-srcRate", 100000);
        final int auctionSrcCycle = params.getInt("auction-srcCycle", 60);

        final int personSrcRate = params.getInt("person-srcRate", 100000);
        final int personSrcCycle = params.getInt("person-srcCycle", 60);


        env.setParallelism(params.getInt("p-window", 1));

        DataStream<Tuple2<Long, Person>> persons = env.addSource(new KeyedPersonSourceFunction(personSrcRate, personSrcCycle))
                .name("Custom Source: Persons")
                .setParallelism(params.getInt("p-person-source", 1))
                .assignTimestampsAndWatermarks(new PersonTimestampAssigner())
                .keyBy(0);

        DataStream<Tuple2<Long, Auction>> auctions = env.addSource(new KeyedAuctionSourceFunction(auctionSrcRate, auctionSrcCycle))
                .name("Custom Source: Auctions")
                .setParallelism(params.getInt("p-auction-source", 1))
                .assignTimestampsAndWatermarks(new AuctionTimestampAssigner())
                .keyBy(0);

        // SELECT Rstream(P.id, P.name, A.reserve)
        // FROM Person [RANGE 1 HOUR] P, Auction [RANGE 1 HOUR] A
        // WHERE P.id = A.seller;
        DataStream<Tuple3<Long, String, Long>> joined =
                persons.join(auctions)
                .where(new KeySelector<Tuple2<Long, Person>, Long>() {
                    @Override
                    public Long getKey(Tuple2<Long, Person> p) {
                        return p.f1.id;
                    }
                }).equalTo(new KeySelector<Tuple2<Long, Auction>, Long>() {
                            @Override
                            public Long getKey(Tuple2<Long, Auction> a) {
                                return a.f1.seller;
                            }
                        })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new FlatJoinFunction<Tuple2<Long, Person>, Tuple2<Long, Auction>, Tuple3<Long, String, Long>>() {
                    @Override
                    public void join(Tuple2<Long, Person> p, Tuple2<Long, Auction> a, Collector<Tuple3<Long, String, Long>> out) {
                        out.collect(new Tuple3<>(p.f1.id, p.f1.name, a.f1.reserve));
                    }
                });

        joined = ((SingleOutputStreamOperator<Tuple3<Long, String, Long>>) joined).disableChaining();

        ((SingleOutputStreamOperator<Tuple3<Long, String, Long>>) joined).setMaxParallelism(params.getInt("mp2", 64));
        ((SingleOutputStreamOperator<Tuple3<Long, String, Long>>) joined).setParallelism(params.getInt("p2",  1));
        ((SingleOutputStreamOperator<Tuple3<Long, String, Long>>) joined).name("join");

        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        joined.transform("DummyLatencySink", objectTypeInfo, new DummyLatencyCountingSink<>(logger))
                .setParallelism(params.getInt("p-window", 1));

        // execute program
        env.execute("Nexmark Query8");
    }

    private static final class PersonTimestampAssigner implements AssignerWithPeriodicWatermarks<Tuple2<Long, Person>> {
        private long maxTimestamp = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(maxTimestamp);
        }

        @Override
        public long extractTimestamp(Tuple2<Long, Person> element, long previousElementTimestamp) {
            maxTimestamp = Math.max(maxTimestamp, element.f1.dateTime);
            return element.f1.dateTime;
        }
    }

    private static final class AuctionTimestampAssigner implements AssignerWithPeriodicWatermarks<Tuple2<Long, Auction>> {
        private long maxTimestamp = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(maxTimestamp);
        }

        @Override
        public long extractTimestamp(Tuple2<Long, Auction> element, long previousElementTimestamp) {
            maxTimestamp = Math.max(maxTimestamp, element.f1.dateTime);
            return element.f1.dateTime;
        }
    }

}