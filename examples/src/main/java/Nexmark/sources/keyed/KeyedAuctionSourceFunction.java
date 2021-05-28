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

import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.nexmark.sources.generator.model.AuctionGenerator;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Random;

/**
 * A ParallelSourceFunction that generates Nexmark Person data
 */
public class KeyedAuctionSourceFunction extends RichParallelSourceFunction<Tuple2<Long, Auction>> {

    private volatile boolean running = true;
    private final GeneratorConfig config = new GeneratorConfig(NexmarkConfiguration.DEFAULT, 1, 1000L, 0, 1);
    private long eventsCountSoFar = 0;
    private int rate;
    private int cycle = 60;

    public KeyedAuctionSourceFunction(int srcRate, int cycle) {
        this.rate = srcRate;
        this.cycle = cycle;
    }

    public KeyedAuctionSourceFunction(int srcRate) {
        this.rate = srcRate;
    }

    @Override
    public void run(SourceContext<Tuple2<Long, Auction>> ctx) throws Exception {
        long streamStartTime = System.currentTimeMillis();

        int epoch = 0;
        int count = 0;
        int curRate = rate;

        while (running && eventsCountSoFar < 70_000_000) {
            long emitStartTime = System.currentTimeMillis();

            if (count == 20) {
                // change input rate every 1 second.
                epoch++;
                curRate = Util.changeRateSin(rate, cycle, epoch);
                System.out.println("epoch: " + epoch%cycle + " current rate is: " + curRate);
                count = 0;
            }

            for (int i = 0; i < Integer.valueOf(curRate/20); i++) {
                long nextId = nextId();
                Random rnd = new Random(nextId);

                // When, in event time, we should generate the event. Monotonic.
                long eventTimestamp =
                        config.timestampAndInterEventDelayUsForEvent(
                                config.nextEventNumber(eventsCountSoFar)).getKey();

                ctx.collect(new Tuple2<>(nextId, AuctionGenerator.nextAuction(eventsCountSoFar, nextId, rnd, eventTimestamp, config)));
                eventsCountSoFar++;
            }

            // Sleep for the rest of timeslice if needed
            Util.pause(emitStartTime);
            count++;
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    private long nextId() {
        return config.firstEventId + config.nextAdjustedEventNumber(eventsCountSoFar);
    }
}