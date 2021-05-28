package flinkapp.wordcount.sources;

import Nexmark.sources.Util;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import static java.lang.Thread.sleep;

public class RateControlledSourceFunctionKV extends RichParallelSourceFunction<Tuple2<String, String>> {

    /** how many sentences to output per second **/
    private int rate;
//    private final int sentenceRate;

    /** the length of each sentence (in chars) **/
    private final int sentenceSize;

    private final RandomSentenceGenerator generator;

    private volatile boolean running = true;

    private int cycle = 60;
    private int base = 0;
    private int warmUpInterval = 100000;

    public RateControlledSourceFunctionKV(int srcRate, int size) {
        rate = srcRate;
        generator = new RandomSentenceGenerator();
        sentenceSize = size;
    }

    public RateControlledSourceFunctionKV(int srcRate, int cycle, int base, int warmUpInterval, int size) {
        this.rate = srcRate;
        this.cycle = cycle;
        this.base = base;
        this.warmUpInterval = warmUpInterval;
        generator = new RandomSentenceGenerator();
        sentenceSize = size;
    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
        int epoch = 0;
        int count = 0;
        int curRate = base + rate;

        // warm up
        Thread.sleep(10000);

        long startTs = System.currentTimeMillis();

        while (running) {
            long emitStartTime = System.currentTimeMillis();
            if (System.currentTimeMillis() - startTs < warmUpInterval) {
                for (int i = 0; i < Integer.valueOf(curRate / 20); i++) {
                    ctx.collect(Tuple2.of(getKey(), generator.nextSentence(sentenceSize)));
                }
                // Sleep for the rest of timeslice if needed
                Util.pause(emitStartTime);
            } else {
                if (count == 20) {
                    // change input rate every 1 second.
//                    epoch++;
                    epoch = (int)((emitStartTime - startTs - warmUpInterval)/1000);
                    curRate = base + Util.changeRateSin(rate, cycle, epoch);
                    System.out.println("epoch: " + epoch % cycle + " current rate is: " + curRate);
                    count = 0;
                }

                for (int i = 0; i < Integer.valueOf(curRate / 20); i++) {
                    ctx.collect(Tuple2.of(getKey(), generator.nextSentence(sentenceSize)));
                }
                // Sleep for the rest of timeslice if needed
                Util.pause(emitStartTime);
                count++;
            }
            ctx.close();
        }
    }



    @Override
    public void cancel() {
        running = false;
    }

    private static String getKey() {
        return String.valueOf(Math.random());
    }
}
