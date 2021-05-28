package kafkagenerator;

import Nexmark.sources.Util;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * SSE generaor
 */
public class WCGenerator {

    private final String TOPIC = "words";

    private static KafkaProducer<String, String> producer;

    private long SENTENCE_NUM = 1000000000;
    private int uniformSize = 10000;
    private double mu = 10;
    private double sigma = 1;

    private int count = 0;

    private transient ListState<Integer> checkpointedCount;

    private int runtime;
    private int nTuples;
    private int nKeys;
    private int rate;

    public WCGenerator(int runtime, int nTuples, int nKeys) {
        this.runtime = runtime;
        this.nTuples = nTuples;
        this.nKeys = nKeys;
        this.rate = nTuples / runtime;
        System.out.println("runtime: " + runtime
                + ", nTuples: " + nTuples
                + ", nKeys: " + nKeys
                + ", rate: " + rate);
        initProducer();
    }

    private void initProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("client.id", "ProducerExample");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    public void generate(int speed) throws InterruptedException {
        RandomDataGenerator messageGenerator = new RandomDataGenerator();
        long time = System.currentTimeMillis();
        long interval = 1000000000/5000;
        long cur = 0;

        long start = System.nanoTime();
        int counter = 0;
        // for loop to generate message
        for (long sent_sentences = 0; sent_sentences < SENTENCE_NUM; ++sent_sentences) {
            cur = System.nanoTime();

            double sentence_length = messageGenerator.nextGaussian(mu, sigma);
            StringBuilder messageBuilder = new StringBuilder();
            for (int l = 0; l < sentence_length; ++l) {
                int number = messageGenerator.nextInt(1, uniformSize);
                messageBuilder.append(String.valueOf(number)).append(" ");
            }

            ProducerRecord<String, String> newRecord = new ProducerRecord<>(TOPIC, messageBuilder.toString());
            producer.send(newRecord);

            counter++;
            // control data generate speed
            while ((System.nanoTime() - cur) < interval) {}
            if (System.nanoTime() - start >= 1000000000) {
                System.out.println("output rate: " + counter);
                counter = 0;
                start = System.nanoTime();
            }
        }
        producer.close();
    }

    public void generate() throws InterruptedException {
        long emitStartTime;

        while (count < nTuples) {
            emitStartTime = System.currentTimeMillis();
            for (int i = 0; i < rate / 20; i++) {
                String key = getChar(count);
                ProducerRecord<String, String> newRecord = new ProducerRecord<>(TOPIC, key);
                producer.send(newRecord);
                count++;
            }

            // Sleep for the rest of timeslice if needed
            Util.pause(emitStartTime);
        }
        producer.close();
    }

    private String getChar(int cur) {
        return "A" + (cur % nKeys);
    }

    public static void main(String[] args) throws InterruptedException {
        final ParameterTool params = ParameterTool.fromArgs(args);

        new WCGenerator(
                params.getInt("runtime", 10),
                params.getInt("nTuples", 10000),
                params.getInt("nKeys", 1000))
                .generate();
    }
}