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

package Nexmark.sources;

import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.nexmark.sources.generator.model.BidGenerator;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

/**
 * A ParallelSourceFunction that generates Nexmark Bid data
 */
public class BidSourceFunction extends RichParallelSourceFunction<Bid> {

    private volatile boolean running = true;
    private GeneratorConfig config;
    private long eventsCountSoFar = 0;
    private int rate;
    private int cycle = 60;
    private int base = 0;
    private int warmUpInterval = 100000;

    public BidSourceFunction(int srcRate, int cycle) {
        this.rate = srcRate;
        this.cycle = cycle;
    }

    public BidSourceFunction(int srcRate, int cycle, int base) {
        this.rate = srcRate;
        this.cycle = cycle;
        this.base = base;
    }

    public BidSourceFunction(int srcRate, int cycle, int base, int warmUpInterval) {
        this.rate = srcRate;
        this.cycle = cycle;
        this.base = base;
        this.warmUpInterval = warmUpInterval;
        NexmarkConfiguration nexconfig = NexmarkConfiguration.DEFAULT;
//        nexconfig.hotBiddersRatio=1;
        nexconfig.hotAuctionRatio=1;
//        nexconfig.numInFlightAuctions=1;
//        nexconfig.numEventGenerators=1;
        config = new GeneratorConfig(nexconfig, 1, 1000L, 0, 1);
    }

    public BidSourceFunction(int srcRate) {
        this.rate = srcRate;
    }

    @Override
    public void run(SourceContext<Bid> ctx) throws Exception {
        long streamStartTime = System.currentTimeMillis();
        int epoch = 0;
        int count = 0;
        int curRate = base + rate;

        // warm up
        Thread.sleep(10000);
//        warmup(ctx);

        long startTs = System.currentTimeMillis();

        while (running) {
            long emitStartTime = System.currentTimeMillis();

            if (System.currentTimeMillis() - startTs < warmUpInterval) {
                for (int i = 0; i < curRate / 20; i++) {

                    long nextId = nextId();
                    Random rnd = new Random(nextId);

                    // When, in event time, we should generate the event. Monotonic.
                    long eventTimestamp =
                            config.timestampAndInterEventDelayUsForEvent(
                                    config.nextEventNumber(eventsCountSoFar)).getKey();

                    ctx.collect(BidGenerator.nextBid(nextId, rnd, eventTimestamp, config));
                    eventsCountSoFar++;
                }

                // Sleep for the rest of timeslice if needed
                Util.pause(emitStartTime);
            } else { // after warm up
                if (count == 20) {
                    // change input rate every 1 second.
                    epoch++;
                    curRate = base + Util.changeRateSin(rate, cycle, epoch);
                    System.out.println("epoch: " + epoch % cycle + " current rate is: " + curRate);
                    count = 0;
                }

                for (int i = 0; i < curRate / 20; i++) {

                    long nextId = nextId();
                    Random rnd = new Random(nextId);

                    // When, in event time, we should generate the event. Monotonic.
                    long eventTimestamp =
                            config.timestampAndInterEventDelayUsForEvent(
                                    config.nextEventNumber(eventsCountSoFar)).getKey();

                    ctx.collect(BidGenerator.nextBid(nextId, rnd, eventTimestamp, config));
                    eventsCountSoFar++;
                }

                // Sleep for the rest of timeslice if needed
                Util.pause(emitStartTime);
                count++;
            }
        }
    }

    private void warmup(SourceContext<Bid> ctx) throws InterruptedException {
        int curRate = rate + base; //  (sin0 + 1)
        long startTs = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTs < warmUpInterval) {
            long emitStartTime = System.currentTimeMillis();
            for (int i = 0; i < curRate / 20; i++) {

                long nextId = nextId();
                Random rnd = new Random(nextId);

                // When, in event time, we should generate the event. Monotonic.
                long eventTimestamp =
                        config.timestampAndInterEventDelayUsForEvent(
                                config.nextEventNumber(eventsCountSoFar)).getKey();

                ctx.collect(BidGenerator.nextBid(nextId, rnd, eventTimestamp, config));
                eventsCountSoFar++;
            }

            // Sleep for the rest of timeslice if needed
            Util.pause(emitStartTime);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    private long nextId() {
        return config.firstEventId + config.nextAdjustedEventNumber(eventsCountSoFar);
//        return config.firstEventId + eventsCountSoFar;
    }
}
