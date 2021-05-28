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
import Nexmark.sinks.DummySink;
import Nexmark.sources.AuctionSourceFunction;
import Nexmark.sources.PersonSourceFunction;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
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

public class Query8 {

    private static final Logger logger  = LoggerFactory.getLogger(Query8.class);

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new MemoryStateBackend(100000000));
//        new MemoryStateBackend(1024 * 1024 * 1024, false);
//        env.setStateBackend(new FsStateBackend("file:///home/myc/workspace/flink-related/states"));
//        env.setStateBackend(new FsStateBackend("hdfs://camel:9000/flink/checkpoints"));

//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        env.getConfig().setAutoWatermarkInterval(1000);

        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // enable latency tracking
        //env.getConfig().setLatencyTrackingInterval(5000);

        final int auctionSrcRate = params.getInt("auction-srcRate", 100000);
        final int auctionSrcCycle = params.getInt("auction-srcCycle", 60);
        final int auctionSrcBase = params.getInt("auction-srcBase", 0);

        final int personSrcRate = params.getInt("person-srcRate", 100000);
        final int personSrcCycle = params.getInt("person-srcCycle", 60);
        final int personSrcBase = params.getInt("person-srcBase", 0);

        env.setParallelism(params.getInt("p-window", 1));

        DataStream<Auction> auctions = env.addSource(new AuctionSourceFunction(auctionSrcRate, auctionSrcCycle, auctionSrcBase))
                .name("Custom Source: Auctions")
                .setParallelism(params.getInt("p-auction-source", 1));
//                .assignTimestampsAndWatermarks(new AuctionTimestampAssigner());

        DataStream<Person> persons = env.addSource(new PersonSourceFunction(personSrcRate, personSrcCycle, personSrcBase))
                .name("Custom Source: Persons")
                .setParallelism(params.getInt("p-person-source", 1));
//                .assignTimestampsAndWatermarks(new PersonTimestampAssigner());



        // SELECT Rstream(P.id, P.name, A.reserve)
        // FROM Person [RANGE 1 HOUR] P, Auction [RANGE 1 HOUR] A
        // WHERE P.id = A.seller;
        DataStream<Tuple3<Long, String, Long>> joined =
                persons.join(auctions)
                        .where(new KeySelector<Person, Long>() {
                            @Override
                            public Long getKey(Person p) {
                                return p.id;
                            }
                        }).equalTo(new KeySelector<Auction, Long>() {
                    @Override
                    public Long getKey(Auction a) {
                        return a.seller;
                    }
                })
                        .window(TumblingEventTimeWindows.of(Time.milliseconds(1)))
                        .apply(new FlatJoinFunction<Person, Auction, Tuple3<Long, String, Long>>() {
                            @Override
                            public void join(Person p, Auction a, Collector<Tuple3<Long, String, Long>> out) {
                                out.collect(new Tuple3<>(p.id, p.name, a.reserve));
                            }
                        });

        joined = ((SingleOutputStreamOperator<Tuple3<Long, String, Long>>) joined).disableChaining();

        ((SingleOutputStreamOperator<Tuple3<Long, String, Long>>) joined).setMaxParallelism(params.getInt("mp2", 64));
        ((SingleOutputStreamOperator<Tuple3<Long, String, Long>>) joined).setParallelism(params.getInt("p2",  1));
        ((SingleOutputStreamOperator<Tuple3<Long, String, Long>>) joined).name("join");

        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        joined.transform("DummySink", objectTypeInfo, new DummySink<>())
				.uid("dummy-sink")
                .setParallelism(params.getInt("p-window", 1));

        // execute program
        env.execute("Nexmark Query8");
    }

    private static final class PersonTimestampAssigner implements AssignerWithPeriodicWatermarks<Person> {
        private long maxTimestamp = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(maxTimestamp);
        }

        @Override
        public long extractTimestamp(Person element, long previousElementTimestamp) {
            maxTimestamp = Math.max(maxTimestamp, element.dateTime);
            return element.dateTime;
        }
    }

    private static final class AuctionTimestampAssigner implements AssignerWithPeriodicWatermarks<Auction> {
        private long maxTimestamp = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(maxTimestamp);
        }

        @Override
        public long extractTimestamp(Auction element, long previousElementTimestamp) {
            maxTimestamp = Math.max(maxTimestamp, element.dateTime);
            return element.dateTime;
        }
    }

}