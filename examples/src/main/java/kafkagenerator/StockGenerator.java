package kafkagenerator;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.flink.api.java.utils.ParameterTool;
import static java.lang.Thread.sleep;

/**
 * SSE generaor
 */
public class StockGenerator {

    private String TOPIC;

    private static KafkaProducer<String, String> producer;

    private static final int Order_No = 0;
    private static final int Tran_Maint_Code = 1;
    private static final int Last_Upd_Time = 2;
    private static final int Order_Price = 3;
    private static final int Order_Exec_Vol = 4;
    private static final int Order_Vol = 5;
    private static final int Sec_Code = 6;
    private static final int Trade_Dir = 7;

    public StockGenerator(String input, String brokers) {
        TOPIC = input;
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("client.id", "ProducerExample");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("partitioner.class", "kafka.SSE.SSEPartitioner");
        producer = new KafkaProducer<>(props);

    }

    public void generate(String FILE, int REPEAT, int INTERVAL) throws InterruptedException {

        String sCurrentLine;
        List<String> textList = new ArrayList<>();
        FileReader stream = null;
        // // for loop to generate message
        BufferedReader br = null;
        long cur;
        long start;
        int counter = 0;

        int noRecSleepCnt = 0;
        int sleepCnt = 0;

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
                    System.out.println("output rate: " + counter + " per " + INTERVAL + "ms");
                    counter = 0;
                    cur = System.currentTimeMillis();
                    if (cur < (long) sleepCnt *INTERVAL + start) {
                        sleep(((long) sleepCnt *INTERVAL + start) - cur);
                    } else {
                        System.out.println("rate exceeds" + INTERVAL + "ms.");
                    }
                }

                String[] orderArr = sCurrentLine.split("\\|");

                if (orderArr.length < 7) {
                    continue;
                }

                for (int i=0; i< REPEAT; i++) {
                    ProducerRecord<String, String> newRecord = new ProducerRecord<>(TOPIC, sCurrentLine.split("\\|")[Sec_Code], sCurrentLine);
                    producer.send(newRecord);
                    counter++;
                }
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
        producer.close();
    }

    public static void main(String[] args) throws InterruptedException {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        String TOPIC = params.get("topic", "stock_sb");
        String FILE = params.get("fp", "/home/myc/workspace/datasets/SSE/sb-5min.txt");
        int REPEAT = params.getInt("repeat", 1);
        String BROKERS = params.get("host", "localhost:9092");
        int INTERVAL = params.getInt("interval", 50);

        System.out.println(TOPIC + FILE + REPEAT + BROKERS);

        new StockGenerator(TOPIC, BROKERS).generate(FILE, REPEAT, INTERVAL);
    }
}