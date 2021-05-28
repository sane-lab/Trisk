package stock;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class OrderPool implements Serializable {
    public HashMap<Integer, ArrayList<Order>> buyPool = new HashMap<>();
    public HashMap<Integer, ArrayList<Order>> sellPool = new HashMap<>();

//    OrderPool(HashMap<Integer, ArrayList<String>> curBuyPool, HashMap<Integer, ArrayList<String>> curSellPool) {
//        this.buyPool = curBuyPool;
//        this.sellPool = curSellPool;
//    }

    OrderPool(HashMap<Integer, ArrayList<Order>> curBuyPool, HashMap<Integer, ArrayList<Order>> curSellPool) {
//        for (Map.Entry entry1 : curBuyPool.entrySet()) {
//            int price = (int) entry1.getKey();
//            ArrayList<Order> orderList = (ArrayList<Order>) entry1.getValue();
//            ArrayList<String> buyList = this.buyPool.getOrDefault(price, new ArrayList<>());
//            for (Order order : orderList) {
//                buyList.add(order.toString());
//            }
//            this.buyPool.put(price, buyList);
//        }
//
//        for (Map.Entry entry1 : curSellPool.entrySet()) {
//            int price = (int) entry1.getKey();
//            ArrayList<Order> orderList = (ArrayList<Order>) entry1.getValue();
//            ArrayList<String> sellList = this.sellPool.getOrDefault(price, new ArrayList<>());
//            for (Order order : orderList) {
//                sellList.add(order.toString());
//            }
//            this.sellPool.put(price, sellList);
//        }
        this.buyPool = curBuyPool;
        this.sellPool = curSellPool;
    }
}
