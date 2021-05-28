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
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.nexmark.sources.generator.model.PersonGenerator;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

;

/**
 * A ParallelSourceFunction that generates Nexmark Person data
 */
public class PersonSourceFunction extends RichParallelSourceFunction<Person> {

    private volatile boolean running = true;
    private final GeneratorConfig config = new GeneratorConfig(NexmarkConfiguration.DEFAULT, 1, 1000L, 0, 1);
    private long eventsCountSoFar = 0;
    private int rate;
    private int cycle = 60;
    private int base = 0;
    private int warmUpInterval = 100000;

    public PersonSourceFunction(int srcRate, int cycle) {
        this.rate = srcRate;
        this.cycle = cycle;
    }

    public PersonSourceFunction(int srcRate, int cycle, int base) {
        this.rate = srcRate;
        this.cycle = cycle;
        this.base = base;
    }

    public PersonSourceFunction(int srcRate, int cycle, int base, int warmUpInterval) {
        this.rate = srcRate;
        this.cycle = cycle;
        this.base = base;
        this.warmUpInterval = warmUpInterval;
    }

    public PersonSourceFunction(int srcRate) {
        this.rate = srcRate;
    }

    @Override
    public void run(SourceContext<Person> ctx) throws Exception {
        long streamStartTime = System.currentTimeMillis();

        int epoch = 0;
        int count = 0;
        int curRate = rate;

        // warm up
        Thread.sleep(60000);
        warmup(ctx);

        while (running) {
            long emitStartTime = System.currentTimeMillis();

            if (count == 20) {
                // change input rate every 1 second.
                epoch++;
                curRate = base + Util.changeRateSin(rate, cycle, epoch);
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

                ctx.collect(PersonGenerator.nextPerson(nextId, rnd, eventTimestamp, config));
                eventsCountSoFar++;
            }

            // Sleep for the rest of timeslice if needed
            Util.pause(emitStartTime);
            count++;
        }
    }

    private void warmup(SourceContext<Person> ctx) throws InterruptedException {
        int curRate = rate + base; //  (sin0 + 1) * rate + base
        long startTs = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTs < warmUpInterval) {
            long emitStartTime = System.currentTimeMillis();
            for (int i = 0; i < Integer.valueOf(curRate/20); i++) {
                long nextId = nextId();
                Random rnd = new Random(nextId);

                // When, in event time, we should generate the event. Monotonic.
                long eventTimestamp =
                        config.timestampAndInterEventDelayUsForEvent(
                                config.nextEventNumber(eventsCountSoFar)).getKey();

                ctx.collect(PersonGenerator.nextPerson(nextId, rnd, eventTimestamp, config));
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
    }
}