package stock;

import org.apache.commons.lang3.StringUtils;

import java.util.*;

public class utils {
    public static Map<Integer, ArrayList<Order>> sortMapBykeyDesc(Map<Integer, ArrayList<Order>> oriMap) {
        Map<Integer, ArrayList<Order>> sortedMap = new LinkedHashMap<>();
        try {
            if (oriMap != null && !oriMap.isEmpty()) {
                List<Map.Entry<Integer, ArrayList<Order>>> entryList = new ArrayList<>(oriMap.entrySet());
                Collections.sort(entryList,
                        (o1, o2) -> {
                            int value1 = 0, value2 = 0;
                            try {
                                value1 = o1.getKey();
                                value2 = o2.getKey();
                            } catch (NumberFormatException e) {
                                value1 = 0;
                                value2 = 0;
                            }
                            return value2 - value1;
                        });
                Iterator<Map.Entry<Integer, ArrayList<Order>>> iter = entryList.iterator();
                Map.Entry<Integer, ArrayList<Order>> tmpEntry;
                while (iter.hasNext()) {
                    tmpEntry = iter.next();
                    sortedMap.put(tmpEntry.getKey(), tmpEntry.getValue());
                }
            }
        } catch (Exception e) {
        }
        return sortedMap;
    }

    public static Map<Integer, ArrayList<Order>> sortMapBykeyAsc(Map<Integer, ArrayList<Order>> oriMap) {
        Map<Integer, ArrayList<Order>> sortedMap = new LinkedHashMap<>();
        try {
            if (oriMap != null && !oriMap.isEmpty()) {
                List<Map.Entry<Integer, ArrayList<Order>>> entryList = new ArrayList<>(oriMap.entrySet());
                Collections.sort(entryList,
                        (o1, o2) -> {
                            int value1 = 0, value2 = 0;
                            try {
                                value1 = o1.getKey();
                                value2 = o2.getKey();
                            } catch (NumberFormatException e) {
                                value1 = 0;
                                value2 = 0;
                            }
                            return value1 - value2;
                        });
                Iterator<Map.Entry<Integer, ArrayList<Order>>> iter = entryList.iterator();
                Map.Entry<Integer, ArrayList<Order>> tmpEntry;
                while (iter.hasNext()) {
                    tmpEntry = iter.next();
                    sortedMap.put(tmpEntry.getKey(), tmpEntry.getValue());
                }
            }
        } catch (Exception e) {
        }
        return sortedMap;
    }

    public static ArrayList<Order> strToList(String stateVal) {
        ArrayList<Order> orderList = new ArrayList<>();
        if (stateVal != null) {
            ArrayList<String> orderStrList = new ArrayList<>(Arrays.asList(stateVal.split(",")));
            for (String orderStr : orderStrList) {
                try {
                    String[] orderArr = orderStr.split("\\|");
                    orderList.add(new Order(orderArr[0], orderArr[1], orderArr[2]));
                } catch (Exception e) {
                    System.out.println(orderStr);
                }
            }
        }
        return orderList;
    }

    public static String listToStr(ArrayList<Order> orderList) {
        return  StringUtils.join(orderList, ",");
    }
}
