package stock.sources;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static java.lang.Thread.sleep;

public class SSERealRateSourceFunctionKV extends RichParallelSourceFunction<Tuple3<String, String, Long>> {



    private volatile boolean running = true;


    private static final int Order_No = 0;
    private static final int Tran_Maint_Code = 1;
    private static final int Last_Upd_Time = 2;
    private static final int Order_Price = 3;
    private static final int Order_Exec_Vol = 4;
    private static final int Order_Vol = 5;
    private static final int Sec_Code = 6;
    private static final int Trade_Dir = 7;

    private String FILE;

    public SSERealRateSourceFunctionKV(String FILE) {
        this.FILE = FILE;
    }

    @Override
    public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws Exception {
        String sCurrentLine;
        FileReader stream = null;
        // // for loop to generate message
        BufferedReader br = null;
        long cur = 0;
        long start = 0;
        int counter = 0;

        int noRecSleepCnt = 0;
        int sleepCnt = 0;

//        Thread.sleep(60000);

        try {
            stream = new FileReader(FILE);
            br = new BufferedReader(stream);

            start = System.currentTimeMillis();

            while ((sCurrentLine = br.readLine()) != null) {
//                if (sCurrentLine.equals("CALLAUCTIONEND")) {
//                    // dont let later process be affected
//                    sleepCnt += 30000/50;
//                    System.out.println("output rate: " + counter + " per " + 50 + "ms");
//                    counter = 0;
//
//                    Thread.sleep(30000);
//
//                    for (int i=0; i<128; i++) {
//                        ctx.collect(new Tuple3<>(i + "", "CALLAUCTIONEND", Long.valueOf(i)));
//                    }
//                    sleepCnt += 30000/50;
//                    Thread.sleep(30000);
//                }

                if (sCurrentLine.equals("end")) {
                    sleepCnt++;
                    if (counter == 0) {
                        noRecSleepCnt++;
                        System.out.println("no record in this sleep !" + noRecSleepCnt);
                    }
                    System.out.println("output rate: " + counter);
                    counter = 0;
                    cur = System.currentTimeMillis();
                    if (cur < sleepCnt*50 + start) {
                        sleep((sleepCnt*50 + start) - cur);
                    } else {
                        System.out.println("rate exceeds" + 50 + "ms.");
                    }
                }

                if (sCurrentLine.split("\\|").length < 8) {
                    continue;
                }

                Long ts = System.currentTimeMillis();
                List<String> stockArr = Arrays.asList(sCurrentLine.split("\\|"));

                ctx.collect(new Tuple3<>(stockArr.get(Sec_Code), sCurrentLine, ts));
                counter++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(stream != null) stream.close();
                if(br != null) br.close();
            } catch(IOException ex) {
                ex.printStackTrace();
            }
        }

        ctx.close();
    }

    @Override
    public void cancel() {
        running = false;
    }
}
