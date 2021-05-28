package stock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author by Mao
 * kmeans data structure, and some operator
 */

class Pool {
    /**
     * The user that viewed the page
     */
    private Map<Float, List<Order>> pool = new HashMap<Float, List<Order>>();
    private List<Float> pricePool = new ArrayList<>();

    Pool(Map<Float, List<Order>> poolI, List<Float> pricePoolI) {
        this.pool = poolI;
        this.pricePool = pricePoolI;
    }

    Map<Float, List<Order>> getPool() {
        return this.pool;
    }
    List<Float> getPricePool() {
        return this.pricePool;
    }
}