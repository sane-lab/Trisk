package Nexmark.sources.keyed;

public class Util {
    public void changeRate(int rate, Boolean inc, Integer n) {
        if (inc) {
            rate += n;
        } else {
            if (rate > n) {
                rate -= n;
            }
        }
    }

    public static int changeRateSin(int rate, int cycle, int epoch) {
        double sineValue = Math.sin(Math.toRadians(epoch*360/cycle)) + 1;
        System.out.println(sineValue);

        Double curRate = (sineValue * rate);
        return curRate.intValue();
    }

    public static int changeRateCos(int rate, int cycle, int epoch) {
        double sineValue = Math.cos(Math.toRadians(epoch*360/cycle)) + 1;
        System.out.println(sineValue);

        Double curRate = (sineValue * rate);
        return curRate.intValue();
    }

    public static void pause(long emitStartTime) throws InterruptedException {
        long emitTime = System.currentTimeMillis() - emitStartTime;
        if (emitTime < 1000/20) {
            Thread.sleep(1000/20 - emitTime);
        }
    }
}
