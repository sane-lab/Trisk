package stock.sources;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.Thread.sleep;

public class SSERealRateSourceFunctionKVInteger extends RichParallelSourceFunction<Tuple3<Integer, String, Long>> {



    private volatile boolean running = true;


    private static final int Order_No = 0;
    private static final int Tran_Maint_Code = 1;
    private static final int Order_Price = 8;
    private static final int Order_Exec_Vol = 9;
    private static final int Order_Vol = 10;
    private static final int Sec_Code = 11;
    private static final int Trade_Dir = 22;

    private String FILE;

    public SSERealRateSourceFunctionKVInteger(String FILE) {
        this.FILE = FILE;
    }

    @Override
    public void run(SourceContext<Tuple3<Integer, String, Long>> ctx) throws Exception {
        String sCurrentLine;
        List<String> textList = new ArrayList<>();
        FileReader stream = null;
        // // for loop to generate message
        BufferedReader br = null;
        int sent_sentences = 0;
        long cur = 0;
        long start = 0;
        int counter = 0;

        int noRecSleepCnt = 0;
        int sleepCnt = 0;

        Thread.sleep(60000);

        try {
            stream = new FileReader(FILE);
            br = new BufferedReader(stream);

            start = System.currentTimeMillis();

            while ((sCurrentLine = br.readLine()) != null) {

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
//                    start = System.currentTimeMillis();
                }

                if (sCurrentLine.split("\\|").length < 10) {
                    continue;
                }

                Long ts = System.currentTimeMillis();
                String msg = sCurrentLine;
                List<String> stockArr = Arrays.asList(msg.split("\\|"));

                ctx.collect(new Tuple3<>(Integer.valueOf(stockArr.get(Sec_Code)), msg, ts));
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
