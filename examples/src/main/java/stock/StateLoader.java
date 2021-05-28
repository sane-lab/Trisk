package stock;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class StateLoader {
    private static final int Order_No = 0;
    private static final int Tran_Maint_Code = 1;
    private static final int Order_Price = 8;
    private static final int Order_Exec_Vol = 9;
    private static final int Order_Vol = 10;
    private static final int Sec_Code = 11;
    private static final int Trade_Dir = 22;

    private static final String INPUT_STREAM_ID = "stock_sb";
    private static final String OUTPUT_STREAM_ID = "stock_cj";
    private static final String KAFKA_BROKERS = "localhost:9092";

    private static final String FILTER_KEY1 = "D";
    private static final String FILTER_KEY2 = "X";
    private static final String FILTER_KEY3 = "";

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStateBackend(new FsStateBackend("file:///home/samza/states"));
//        env.setStateBackend(new FsStateBackend("file:///home/myc/workspace/flink-related/states"));

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        FlinkKafkaProducer011<Tuple2<String, String>> kafkaProducer = new FlinkKafkaProducer011<Tuple2<String, String>>(
                KAFKA_BROKERS, OUTPUT_STREAM_ID, new KafkaWithTsMsgSchema());
        kafkaProducer.setWriteTimestampToKafka(true);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", params.get("kafka", KAFKA_BROKERS));
        FlinkKafkaConsumer011<Tuple3<String, String, Long>> inputConsumer = new FlinkKafkaConsumer011<>(
                INPUT_STREAM_ID, new KafkaWithTsMsgSchema(), kafkaProps);

        inputConsumer.setStartFromLatest();
        inputConsumer.setCommitOffsetsOnCheckpoints(false);

        final DataStream<Tuple3<String, String, Long>> text = env.addSource(
                inputConsumer).setMaxParallelism(params.getInt("mp2", 64));

        // split up the lines in pairs (2-tuples) containing:
        DataStream<Tuple2<String, String>> counts = text.keyBy(0)
                .flatMap(new MatchMaker())
                .name("MatchMaker FlatMap")
                .uid("flatmap")
                .setMaxParallelism(params.getInt("mp2", 64))
                .setParallelism(params.getInt("p2", 3))
                .keyBy(0);

        counts.addSink(kafkaProducer)
                .name("Sink")
                .uid("sink")
                .setParallelism(params.getInt("p3", 1));

        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        // execute program
        env.execute("Stock Exchange");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    public static final class MatchMaker extends RichFlatMapFunction<Tuple3<String, String, Long>, Tuple2<String, String>> {
        private static final long serialVersionUID = 1L;

        private Map<String, String> stockExchangeMapSell = new HashMap<>();
        private Map<String, String> stockExchangeMapBuy = new HashMap<>();
        private RandomDataGenerator randomGen = new RandomDataGenerator();
        long start = System.currentTimeMillis();
        long latency = 0;
        int tuples = 0;
        int epoch = 0;
        private transient MapState<String, String> countMap;

        @Override
        public void open(Configuration config) {
            MapStateDescriptor<String, String> descriptor =
                    new MapStateDescriptor<>("matchmaker", String.class, String.class);

            countMap = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void flatMap(Tuple3<String, String, Long> value, Collector<Tuple2<String, String>> out) throws Exception {
            String stockOrder = (String) value.f1;
            String[] orderArr = stockOrder.split("\\|");

            delay(5);

            if (orderArr[Tran_Maint_Code].equals(FILTER_KEY1) || orderArr[Tran_Maint_Code].equals(FILTER_KEY2) || orderArr[Tran_Maint_Code].equals(FILTER_KEY3)) {
                return;
            }

            countMap.put(orderArr[Sec_Code], stockOrder);

            Map<String, String> matchedResult = doStockExchange(orderArr, orderArr[Trade_Dir]);

//            latency += System.currentTimeMillis() - value.f2;
//            System.out.println("stock_id: " + value.f0 + " arrival_ts: " + value.f2 + " completion_ts: " + System.currentTimeMillis());
//            tuples++;
//            if (System.currentTimeMillis() - start >= 1000) {
//                start = System.currentTimeMillis();
//                float avg_latency = (float) latency / tuples;
//                epoch++;
//                tuples = 0;
//                latency = 0;
//                System.out.println("latency: " + avg_latency + " ts: " + System.nanoTime());
////                List<String> latency = Arrays.asList(String.valueOf(avg_latency));
//
////                Path latencyFile = Paths.get("./latency.log").toAbsolutePath();
////                try {
////                    Files.write(latencyFile, latency, Charset.forName("UTF-8"));
////                } catch (IOException e) {
////                    System.err.println("Error while writing latency file for epoch " + epoch + ".");
////                    e.printStackTrace();
////                }
//            }
            out.collect(new Tuple2<>(value.f0, value.f1));
        }

        public Map<String, String> doStockExchange(String[] orderArr, String direction) {
            Map<String, String> matchedResult = new HashMap<>();
            if (direction.equals("")) {
                System.out.println("bad tuple received!");
                return matchedResult;
            }
            if (direction.equals("S")) {
                stockExchangeMapSell.put(orderArr[Sec_Code], String.join("|", orderArr));
                matchedResult = tradeSell(orderArr, stockExchangeMapBuy);
            } else {
                stockExchangeMapBuy.put(orderArr[Sec_Code], String.join("|", orderArr));
                matchedResult = tradeBuy(orderArr, stockExchangeMapSell);
            }
            return matchedResult;
        }

        private Map<String, String> tradeSell(String[] sellerOrder, Map<String, String> stockExchangeMap) {
            Map<String, String> matchedBuy = new HashMap<>();
            Map<String, String> matchedSell = new HashMap<>();
            Map<String, String> pendingBuy = new HashMap<>();
            Map<String, String> pendingSell = new HashMap<>();
            Iterator iter = stockExchangeMap.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<String, String> entry = (Map.Entry<String, String>) iter.next();
                String orderNo = entry.getKey();
                String[] curBuyerOrder = entry.getValue().split("\\|");

                if (curBuyerOrder[Sec_Code].equals(sellerOrder[Sec_Code])) {
                    String left = match(curBuyerOrder, sellerOrder);
                    if (!left.equals("unmatched")) {
                        if (left.equals("S")) {
                            pendingSell.put(sellerOrder[Sec_Code], String.join("\\|", sellerOrder));
                            matchedBuy.put(curBuyerOrder[Sec_Code], String.join("\\|", curBuyerOrder));
                        } else if (left.equals("B")) {
                            pendingBuy.put(curBuyerOrder[Sec_Code], String.join("\\|", curBuyerOrder));
                            matchedSell.put(sellerOrder[Sec_Code], String.join("\\|", sellerOrder));
                        } else {
                            matchedSell.put(sellerOrder[Sec_Code], String.join("\\|", sellerOrder));
                            matchedBuy.put(curBuyerOrder[Sec_Code], String.join("\\|", curBuyerOrder));
                        }
                    }
                }
            }

            updateStore(pendingBuy, pendingSell, matchedBuy, matchedSell);

            return matchedSell;
        }

        private Map<String, String> tradeBuy(String[] buyerOrder, Map<String, String> stockExchangeMap) {
            Map<String, String> matchedBuy = new HashMap<>();
            Map<String, String> matchedSell = new HashMap<>();
            Map<String, String> pendingBuy = new HashMap<>();
            Map<String, String> pendingSell = new HashMap<>();
            Iterator iter = stockExchangeMap.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<String, String> entry = (Map.Entry<String, String>) iter.next();
                String orderNo = entry.getKey();
                String[] curSellerOrder = entry.getValue().split("\\|");

                if (curSellerOrder[Sec_Code].equals(buyerOrder[Sec_Code])) {
                    String left = match(buyerOrder, curSellerOrder);
                    if (!left.equals("unmatched")) {
                        if (left.equals("S")) {
                            pendingSell.put(curSellerOrder[Sec_Code], String.join("\\|", curSellerOrder));
                            matchedBuy.put(buyerOrder[Sec_Code], String.join("\\|", buyerOrder));
                        } else if (left.equals("B")) {
                            pendingBuy.put(buyerOrder[Sec_Code], String.join("\\|", buyerOrder));
                            matchedSell.put(curSellerOrder[Sec_Code], String.join("\\|", curSellerOrder));
                        } else {
                            matchedSell.put(curSellerOrder[Sec_Code], String.join("\\|", curSellerOrder));
                            matchedBuy.put(buyerOrder[Sec_Code], String.join("\\|", buyerOrder));
                        }
                    }
                }
            }

            updateStore(pendingBuy, pendingSell, matchedBuy, matchedSell);

            return matchedSell;
        }

        private String match(String[] buyerOrder, String[] sellerOrder) {
            float buyPrice = Float.valueOf(buyerOrder[Order_Price]);
            float sellPrice = Float.valueOf(sellerOrder[Order_Price]);
            if (buyPrice < sellPrice) {
                return "unmatched";
            }
            float buyVol = Float.valueOf(buyerOrder[Order_Vol]);
            float sellVol = Float.valueOf(sellerOrder[Order_Vol]);
            if (buyVol > sellVol) {
                buyerOrder[Order_Vol] = String.valueOf(buyVol - sellVol);
                return "B";
            } else if (buyVol < sellVol) {
                sellerOrder[Order_Vol] = String.valueOf(sellVol - buyVol);
                return "S";
            } else {
                return "";
            }
        }

        private void updateStore(
                Map<String, String> pendingBuy,
                Map<String, String> pendingSell,
                Map<String, String> matchedBuy,
                Map<String, String> matchedSell) {
            for (Map.Entry<String, String> order : pendingBuy.entrySet()) {
                stockExchangeMapBuy.put(order.getKey(), order.getValue());
            }
            for (Map.Entry<String, String> order : pendingSell.entrySet()) {
                stockExchangeMapSell.put(order.getKey(), order.getValue());
            }
            for (Map.Entry<String, String> order : matchedBuy.entrySet()) {
                stockExchangeMapBuy.remove(order.getKey());
            }
            for (Map.Entry<String, String> order : matchedSell.entrySet()) {
                stockExchangeMapSell.remove(order.getKey());
            }
        }

        private void delay(int interval) {
            Double ranN = randomGen.nextGaussian(interval, 1);
            ranN = ranN*1000000;
            long delay = ranN.intValue();
            if (delay < 0) delay = 6000000;
            Long start = System.nanoTime();
            while (System.nanoTime() - start < delay) {}
        }
    }
}
