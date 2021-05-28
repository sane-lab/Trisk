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

package Nexmark.sources.keyed;

import Nexmark.sources.Util;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.nexmark.sources.generator.model.BidGenerator;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Random;

/**
 * A ParallelSourceFunction that generates Nexmark Bid data
 */
public class KeyedBidSourceFunction extends RichParallelSourceFunction<Tuple2<Long, Bid>> {

    private volatile boolean running = true;
    private final GeneratorConfig config = new GeneratorConfig(NexmarkConfiguration.DEFAULT, 1, 1000L, 0, 1);
    private long eventsCountSoFar = 0;
    private int rate;
    private int cycle = 60;
    private int base = 0;
    private int warmUpInterval = 100000;

    public KeyedBidSourceFunction(int srcRate, int cycle) {
        this.rate = srcRate;
        this.cycle = cycle;
    }

    public KeyedBidSourceFunction(int srcRate, int cycle, int base) {
        this.rate = srcRate;
        this.cycle = cycle;
        this.base = base;
    }

    public KeyedBidSourceFunction(int srcRate, int cycle, int base, int warmUpInterval) {
        this.rate = srcRate;
        this.cycle = cycle;
        this.base = base;
        this.warmUpInterval = warmUpInterval;
    }

    public KeyedBidSourceFunction(int srcRate) {
        this.rate = srcRate;
    }

    @Override
    public void run(SourceContext<Tuple2<Long, Bid>> ctx) throws Exception {
        long streamStartTime = System.currentTimeMillis();
        int epoch = 0;
        int count = 0;
        int curRate = base + rate;

        // warm up
        Thread.sleep(60000);
//        warmup(ctx);

        long startTs = System.currentTimeMillis();

        while (running) {
            long emitStartTime = System.currentTimeMillis();

            if (System.currentTimeMillis() - startTs < warmUpInterval) {
                for (int i = 0; i < Integer.valueOf(curRate / 20); i++) {

                    long nextId = nextId();
                    Random rnd = new Random(nextId);

                    // When, in event time, we should generate the event. Monotonic.
                    long eventTimestamp =
                            config.timestampAndInterEventDelayUsForEvent(
                                    config.nextEventNumber(eventsCountSoFar)).getKey();

                    ctx.collect(new Tuple2<>(nextId, BidGenerator.nextBid(nextId, rnd, eventTimestamp, config)));
                    eventsCountSoFar++;
                }

                // Sleep for the rest of timeslice if needed
                Nexmark.sources.Util.pause(emitStartTime);
            } else { // after warm up
                if (count == 20) {
                    // change input rate every 1 second.
                    epoch++;
                    curRate = base + Nexmark.sources.Util.changeRateSin(rate, cycle, epoch);
                    System.out.println("epoch: " + epoch % cycle + " current rate is: " + curRate);
                    count = 0;
                }

                for (int i = 0; i < Integer.valueOf(curRate / 20); i++) {

                    long nextId = nextId();
                    Random rnd = new Random(nextId);

                    // When, in event time, we should generate the event. Monotonic.
                    long eventTimestamp =
                            config.timestampAndInterEventDelayUsForEvent(
                                    config.nextEventNumber(eventsCountSoFar)).getKey();

                    // to make it has stable rate among different task
                    ctx.collect(new Tuple2<>(getLong(), BidGenerator.nextBid(nextId, rnd, eventTimestamp, config)));
                    eventsCountSoFar++;
                }

                // Sleep for the rest of timeslice if needed
                Nexmark.sources.Util.pause(emitStartTime);
                count++;
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    private long nextId() {
        return config.firstEventId + config.nextAdjustedEventNumber(eventsCountSoFar);
    }

    private static long getLong() {
        return (long) Math.random();
    }
}