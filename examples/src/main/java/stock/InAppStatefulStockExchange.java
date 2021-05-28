package stock;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import stock.sources.SSERealRateSourceFunctionKV;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static stock.utils.sortMapBykeyAsc;
import static stock.utils.sortMapBykeyDesc;

public class InAppStatefulStockExchange {
    private static final int Order_No = 0;
    private static final int Tran_Maint_Code = 1;
    private static final int Last_Upd_Time = 2;
    private static final int Order_Price = 3;
    private static final int Order_Exec_Vol = 4;
    private static final int Order_Vol = 5;
    private static final int Sec_Code = 6;
    private static final int Trade_Dir = 7;

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

        env.setStateBackend(new MemoryStateBackend(100000000));

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        env.getConfig().registerKryoType(Order.class);


//        final DataStream<Tuple3<String, String, Long>> text = env.addSource(
//                inputConsumer).setMaxParallelism(params.getInt("mp2", 64));

        final DataStream<Tuple3<String, String, Long>> text = env.addSource(
                new SSERealRateSourceFunctionKV(
                        params.get("source-file", "/home/myc/workspace/datasets/SSE/sb-5min.txt")))
                .uid("sentence-source")
                .setParallelism(params.getInt("p1", 1))
                .setMaxParallelism(params.getInt("mp2", 128));


        // split up the lines in pairs (2-tuples) containing:
        DataStream<Tuple2<String, String>> txns = text.keyBy(tuple -> tuple.f0)
                .flatMap(new MatchMaker())
                .name("MatchMaker FlatMap")
                .uid("flatmap")
                .setMaxParallelism(params.getInt("mp2", 128))
                .setParallelism(params.getInt("p2", 3))
                .keyBy(0);

//        txns.print();
//        txns.addSink(kafkaProducer)
//                .name("Sink")
//                .uid("sink")
//                .setParallelism(params.getInt("p3", 1));

        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        // execute program
        env.execute("Stock Exchange");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    public static final class MatchMaker extends RichFlatMapFunction<Tuple3<String, String, Long>, Tuple2<String, String>> {
        private static final long serialVersionUID = 1L;

        private MapState<String, OrderPool> pool;

        private RandomDataGenerator randomGen = new RandomDataGenerator();

        // pool is a architecture used to do stock transaction, we can use collction.sort to sort orders by price.
        // then we need to sort order by timestamp, im not sure how to do this now...
        private Map<String, HashMap<Integer, ArrayList<Order>>> poolS = new HashMap<>();
        private Map<String, HashMap<Integer, ArrayList<Order>>> poolB = new HashMap<>();

        private int continuousAuction = 92500;
        private boolean callAuctionAllowed = true;

        private boolean isPoolLoaded = false;

        @Override
        public void open(Configuration config) {
            MapStateDescriptor<String, OrderPool> testDescriptor =
                    new MapStateDescriptor<>("pool",
                            TypeInformation.of(new TypeHint<String>() {}), TypeInformation.of(new TypeHint<OrderPool>() {}));

            pool = getRuntimeContext().getMapState(testDescriptor);
        }

        @Override
        public void flatMap(Tuple3<String, String, Long> value, Collector<Tuple2<String, String>> out) throws Exception {
            String stockId = value.f0;
            String stockOrder = value.f1;
            String[] orderArr = stockOrder.split("\\|");

//            if (stockOrder.equals("CALLAUCTIONEND") && callAuctionAllowed) {
//                // start to do call auction
//                callAuction();
//                callAuctionAllowed = false;
//                return;
//            }
//
//            if (stockOrder.equals("CALLAUCTIONEND")) {
//                return;
//            }

            //filter
            if (orderArr[Tran_Maint_Code].equals(FILTER_KEY2) || orderArr[Tran_Maint_Code].equals(FILTER_KEY3)) {
                return;
            }

            int curTime = Integer.parseInt(orderArr[Last_Upd_Time].replace(":", ""));
            Order curOrder = new Order(orderArr);
            if (curTime < continuousAuction) {
                // store all orders at maps
                if (orderArr[Tran_Maint_Code].equals("D")) {
                    deleteOrderFromPool(curOrder, orderArr[Sec_Code], orderArr[Trade_Dir]);
                } else {
                    insertPool(curOrder, orderArr[Sec_Code], orderArr[Trade_Dir]);
                }
            } else {
                delay(8);
                Map<String, String> matchedResult = continuousStockExchange(orderArr, orderArr[Trade_Dir]);
            }

            System.out.println("ts: " + value.f2 + " endToEnd latency: " + (System.currentTimeMillis() - value.f2));

            out.collect(new Tuple2<>(value.f0, value.f1));
        }

        public void callAuction() throws Exception {
            // do call auction
            // 1. sort buy order and sell order by price and timestamp
            System.out.println("Start call auction");
//        loadPool();

            // 2. do stock exchange on every stock id
            for (Map.Entry poolBentry : poolB.entrySet()) {
                String curStockId = (String) poolBentry.getKey();
                // filter for debug
                HashMap<Integer, ArrayList<Order>> curBuyPool = (HashMap<Integer, ArrayList<Order>>) poolBentry.getValue();
                HashMap<Integer, ArrayList<Order>> sortedCurBuyPool = (HashMap<Integer, ArrayList<Order>>) sortMapBykeyDesc(curBuyPool);
                // for sorted prices, do stock exchange
                for (Map.Entry curBuyOrdersEntry : sortedCurBuyPool.entrySet()) {
                    int curBuyPrice = (int) curBuyOrdersEntry.getKey();
                    ArrayList<Order> curBuyOrders = (ArrayList<Order>) curBuyOrdersEntry.getValue();

                    // get the sell orders from sell pool
                    HashMap<Integer, ArrayList<Order>> curSellPool = poolS.getOrDefault(curStockId, new HashMap<>());
                    // buyer list should descending, seller should be ascending
                    HashMap<Integer, ArrayList<Order>> sortedCurSellPool = (HashMap<Integer, ArrayList<Order>>) sortMapBykeyAsc(curSellPool);

                    // match orders
                    for (Map.Entry curSellOrdersEntry : sortedCurSellPool.entrySet()) {
                        int curSellPrice = (int) curSellOrdersEntry.getKey();
                        // when matched, do transaction
                        if (curBuyPrice >= curSellPrice) {
                            ArrayList<Order> curSellOrders = (ArrayList<Order>) curSellOrdersEntry.getValue();
                            stockExchange(curBuyOrders, curSellOrders);
                            // add pending orders into pool again for future usage
                            updatePool(curSellPool, curSellOrders, curSellPrice);
                        }
                    }
                    // put updated sell pool into original pool
                    poolS.replace(curStockId, curSellPool);
                    updatePool(curBuyPool, curBuyOrders, curBuyPrice);
                }
                // put updated buy pool into original pool
                poolB.replace(curStockId, curBuyPool);
                metricsDump();
            }

            allStockFlush();
        }

        public Map<String, String> continuousStockExchange(String[] orderArr, String direction) throws Exception {
            long start = System.nanoTime();
            Map<String, String> matchedResult = new HashMap<>();
            String stockId = orderArr[Sec_Code];
            Order curOrder = new Order(orderArr);
            metricsDump();

            // delete stock order, index still needs to be deleted
            if (orderArr[Tran_Maint_Code].equals(FILTER_KEY1)) {
                deleteOrder(curOrder, stockId, direction);
                return matchedResult;
            }
            if (direction.equals("")) {
                System.out.println("bad tuple received!");
                return matchedResult;
            }

            HashMap<Integer, ArrayList<Order>> curSellPool = new HashMap<>();
            HashMap<Integer, ArrayList<Order>> curBuyPool = new HashMap<>();

//            strToOrder(stockId, curSellPool, curBuyPool);

            if (pool.contains(stockId)) {
                curSellPool = pool.get(stockId).sellPool;
                curBuyPool = pool.get(stockId).buyPool;
            } else {
                curSellPool = new HashMap<>();
                curBuyPool = new HashMap<>();
            }

            if (direction.equals("B")) {
                int curBuyPrice = curOrder.getOrderPrice();
                // put into state and index
                ArrayList<Order> curBuyOrders = curBuyPool.getOrDefault(curBuyPrice, new ArrayList<>());
                curBuyOrders.add(curOrder);
                curBuyPool.put(curBuyPrice, curBuyOrders);

                // do partial transaction
                HashMap<Integer, ArrayList<Order>> sortedCurSellPool = (HashMap<Integer, ArrayList<Order>>)
                        sortMapBykeyAsc(curSellPool);

                // match orders
                for (Map.Entry curSellOrdersEntry : sortedCurSellPool.entrySet()) {
                    int curSellPrice = (int) curSellOrdersEntry.getKey();
                    // when matched, do transaction
                    if (curBuyPrice >= curSellPrice) {
//                    isMatched = true;
                        ArrayList<Order> curSellOrders = (ArrayList<Order>) curSellOrdersEntry.getValue();
                        stockExchange(curBuyOrders, curSellOrders);
                        // add pending orders into pool again for future usage
                        updatePool(curSellPool, curSellOrders, curSellPrice);
                    }
                }
                updatePool(curBuyPool, curBuyOrders, curBuyPrice);
            } else {
                int curSellPrice = curOrder.getOrderPrice();
                ArrayList<Order> curSellOrders = curSellPool.getOrDefault(curSellPrice, new ArrayList<>());
                curSellOrders.add(curOrder);
                curSellPool.put(curSellPrice, curSellOrders);

                // do partial transaction
                HashMap<Integer, ArrayList<Order>> sortedCurBuyPool = (HashMap<Integer, ArrayList<Order>>)
                        sortMapBykeyDesc(curBuyPool);
                // match orders
                for (Map.Entry curBuyOrdersEntry : sortedCurBuyPool.entrySet()) {
                    int curBuyPrice = (int) curBuyOrdersEntry.getKey();
                    // when matched, do transaction
                    if (curBuyPrice >= curSellPrice) {
//                    isMatched = true;
                        ArrayList<Order> curBuyOrders = (ArrayList<Order>) curBuyOrdersEntry.getValue();
                        stockExchange(curBuyOrders, curSellOrders);
                        // add pending orders into pool again for future usage
                        updatePool(curBuyPool, curBuyOrders, curBuyPrice);
                    }
                }
                updatePool(curSellPool, curSellOrders, curSellPrice);
            }

            // insert into state
            pool.put(stockId, new OrderPool(curBuyPool, curSellPool));

            System.out.println("stockid: " + stockId + " processing time: " + (System.nanoTime() - start));
            return matchedResult;
        }

        public void deleteOrder(Order curOrder, String stockId, String direction) throws Exception {
            if (direction.equals("")) {
                System.out.println("no order to delete!");
            }
            HashMap<Integer, ArrayList<Order>> curSellPool = new HashMap<>();
            HashMap<Integer, ArrayList<Order>> curBuyPool = new HashMap<>();

//            strToOrder(stockId, curSellPool, curBuyPool);

            if (pool.contains(stockId)) {
                curSellPool = pool.get(stockId).sellPool;
                curBuyPool = pool.get(stockId).buyPool;
            } else {
                curSellPool = new HashMap<>();
                curBuyPool = new HashMap<>();
            }

            int orderPrice = curOrder.getOrderPrice();
            int orderNo = curOrder.getOrderNo();

            Order targetOrder = null;

            if (direction.equals("S")) {
                ArrayList<Order> curSellOrders = curSellPool.getOrDefault(orderPrice, new ArrayList<>());
                for (Order order : curSellOrders) {
                    if (order.getOrderNo() == orderNo) {
                        targetOrder = order;
                        break;
                    }
                }
                curSellOrders.remove(targetOrder);
                updatePool(curSellPool, curSellOrders, orderPrice);
            }
            if (direction.equals("B")) {
                ArrayList<Order> curBuyOrders = curBuyPool.getOrDefault(orderPrice, new ArrayList<>());
                for (Order order : curBuyOrders) {
                    if (order.getOrderNo() == orderNo) {
                        targetOrder = order;
                        break;
                    }
                }
                curBuyOrders.remove(targetOrder);
                updatePool(curBuyPool, curBuyOrders, orderPrice);
            }

            pool.put(stockId, new OrderPool(curBuyPool, curSellPool));
        }

        private void strToOrder(String stockId, HashMap<Integer, ArrayList<Order>> curSellPool, HashMap<Integer, ArrayList<Order>> curBuyPool) throws Exception {
//            HashMap<Integer, ArrayList<String>> sellPool;
//            HashMap<Integer, ArrayList<String>> buyPool;
//            if (pool.contains(stockId)) {
//                sellPool = pool.get(stockId).sellPool;
//                buyPool = pool.get(stockId).buyPool;
//            } else {
//                sellPool = new HashMap<>();
//                buyPool = new HashMap<>();
//            }
//
//            for (Map.Entry entry1 : buyPool.entrySet()) {
//                int price = (int) entry1.getKey();
//                ArrayList<String> orderList = (ArrayList<String>) entry1.getValue();
//                ArrayList<Order> buyList = curBuyPool.getOrDefault(price, new ArrayList<>());
//                for (String orderStr : orderList) {
//                    String[] orderArr = orderStr.split("\\|");
//                    buyList.add(new Order(orderArr[0], orderArr[1], orderArr[2]));
//                }
//                curBuyPool.put(price, buyList);
//            }
//
//            for (Map.Entry entry1 : sellPool.entrySet()) {
//                int price = (int) entry1.getKey();
//                ArrayList<String> orderList = (ArrayList<String>) entry1.getValue();
//                ArrayList<Order> sellList = curSellPool.getOrDefault(price, new ArrayList<>());
//                for (String orderStr : orderList) {
//                    String[] orderArr = orderStr.split("\\|");
//                    sellList.add(new Order(orderArr[0], orderArr[1], orderArr[2]));
//                }
//                curSellPool.put(price, sellList);
//            }

            if (pool.contains(stockId)) {
                curSellPool = pool.get(stockId).sellPool;
                curBuyPool = pool.get(stockId).buyPool;
            } else {
                curSellPool = new HashMap<>();
                curBuyPool = new HashMap<>();
            }
        }

        public void updatePool(HashMap<Integer, ArrayList<Order>> curPool, ArrayList<Order> orderList, int key) {
            if (orderList.isEmpty()) {
                curPool.remove(key);
            } else {
                curPool.replace(key, orderList);
            }
        }

        public void insertPool(Order curOrder, String stockId, String direction) {
            int curOrderPrice = curOrder.getOrderPrice();

            if (direction.equals("B")) {
                HashMap<Integer, ArrayList<Order>> curPool = poolB.getOrDefault(stockId, new HashMap<>());
                ArrayList<Order> curOrderList = curPool.getOrDefault(curOrderPrice, new ArrayList<>());
                // need to keep pool price be sorted, so insert it into pool price
                curOrderList.add(curOrder);
                curPool.put(curOrderPrice, curOrderList);
                poolB.put(stockId, curPool);
            } else {
                HashMap<Integer, ArrayList<Order>> curPool = poolS.getOrDefault(stockId, new HashMap<>());
                ArrayList<Order> curOrderList = curPool.getOrDefault(curOrderPrice, new ArrayList<>());
                // need to keep pool price be sorted, so insert it into pool price
                curOrderList.add(curOrder);
                curPool.put(curOrderPrice, curOrderList);
                poolS.put(stockId, curPool);
            }
        }

        public void deleteOrderFromPool(Order curOrder, String stockId, String direction) {
            if (direction.equals("")) {
                System.out.println("no order to delete!");
            }

            int orderPrice = curOrder.getOrderPrice();
            int orderNo = curOrder.getOrderNo();

            Order targetOrder = null;

            if (direction.equals("S")) {
                HashMap<Integer, ArrayList<Order>> curSellPool = poolS.getOrDefault(stockId, new HashMap<>());
                ArrayList<Order> curSellOrders = curSellPool.getOrDefault(orderPrice, new ArrayList<>());
                for (Order order : curSellOrders) {
                    if (order.getOrderNo() == orderNo) {
                        targetOrder = order;
                        break;
                    }
                }
                curSellOrders.remove(targetOrder);
                updatePool(curSellPool, curSellOrders, orderPrice);
                poolS.replace(stockId, curSellPool);
            }
            if (direction.equals("B")) {
                HashMap<Integer, ArrayList<Order>> curBuyPool = poolB.getOrDefault(stockId, new HashMap<>());
                ArrayList<Order> curBuyOrders = curBuyPool.getOrDefault(orderPrice, new ArrayList<>());
                for (Order order : curBuyOrders) {
                    if (order.getOrderNo() == orderNo) {
                        targetOrder = order;
                        break;
                    }
                }
                curBuyOrders.remove(targetOrder);
                updatePool(curBuyPool, curBuyOrders, orderPrice);
                poolB.replace(stockId,curBuyPool);
            }
        }

        public void stockExchange(ArrayList<Order> curBuyOrders, ArrayList<Order> curSellOrders) {
            ArrayList<Order> tradedBuyOrders = new ArrayList<>();
            ArrayList<Order> tradedSellOrders = new ArrayList<>();

            // match orders one by one, until all orders are matched
            for (Order curBuyOrder : curBuyOrders) {
                for (Order curSellOrder : curSellOrders) {
                    int buyVol = curBuyOrder.getOrderVol();
                    int sellVol = curSellOrder.getOrderVol();
                    if (buyVol == 0 || sellVol == 0) continue;
                    if (buyVol > sellVol) {
                        curBuyOrder.updateOrder(sellVol);
                        curSellOrder.updateOrder(sellVol);
                        tradedSellOrders.add(curSellOrder);
                    } else {
                        curBuyOrder.updateOrder(buyVol);
                        curSellOrder.updateOrder(buyVol);
                        tradedBuyOrders.add(curBuyOrder);
                    }
                }
            }
            // remove traded orders, and update half-traded orders
            for (Order tradedSellOrder : tradedSellOrders) {
                curSellOrders.remove(tradedSellOrder);
            }

            for (Order tradedBuyOrder : tradedBuyOrders) {
                curBuyOrders.remove(tradedBuyOrder);
            }
        }

        public void allStockFlush() throws Exception {
            for (Map.Entry entry : poolS.entrySet()) {
                String stockId = (String) entry.getKey();
                HashMap<Integer, ArrayList<Order>> curSellPool = (HashMap<Integer, ArrayList<Order>>) entry.getValue();
                HashMap<Integer, ArrayList<Order>> curBuyPool = poolB.getOrDefault(stockId, new HashMap<>());
                oneStockFlush(curBuyPool, curSellPool, stockId);
            }
            for (Map.Entry entry : poolB.entrySet()) {
                String stockId = (String) entry.getKey();
                if (!pool.contains(stockId)) {
                    HashMap<Integer, ArrayList<Order>> curBuyPool = (HashMap<Integer, ArrayList<Order>>) entry.getValue();
                    HashMap<Integer, ArrayList<Order>> curSellPool = poolS.getOrDefault(stockId, new HashMap<>());
                    oneStockFlush(curBuyPool, curSellPool, stockId);
                }
            }
        }

        public void oneStockFlush(HashMap<Integer, ArrayList<Order>> curBuyPool,
                                  HashMap<Integer, ArrayList<Order>> curSellPool,
                                  String stockId) throws Exception {
//                pool.put(stockId, new OrderPool(curBuyPool, curSellPool));
        }

        public void metricsDump() throws Exception {
            int sum = 0;
            int buyOrders = 0;
            int sellOrders = 0;
            for (String key : pool.keys()) {
                sum++;
                OrderPool orderPool = pool.get(key);
                for (Map.Entry entry1 : orderPool.sellPool.entrySet()) {
                    ArrayList<Order> orderList = (ArrayList<Order>) entry1.getValue();
                    sellOrders += orderList.size();
                }
                for (Map.Entry entry1 : orderPool.buyPool.entrySet()) {
                    ArrayList<Order> orderList = (ArrayList<Order>) entry1.getValue();
                    buyOrders += orderList.size();
                }
            }

            System.out.println("state size: " + sum + " buy orders: " + buyOrders
                    + " sell orders: " + sellOrders + " total: " + (buyOrders + sellOrders));
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
