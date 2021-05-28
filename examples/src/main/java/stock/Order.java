package stock;

import java.io.Serializable;

/**
 * Author by Mao
 * kmeans data structure, and some operator
 */

class Order implements Serializable {
    /**
     * The user that viewed the page
     */
    private static final int Order_No = 0;
    private static final int Tran_Maint_Code = 1;
    private static final int Last_Upd_Time = 2;
    private static final int Order_Price = 3;
    private static final int Order_Exec_Vol = 4;
    private static final int Order_Vol = 5;
    private static final int Sec_Code = 6;
    private static final int Trade_Dir = 7;

    private static final int Mapped_Order_No = 0;
    private static final int Mapped_Order_Price = 1;
    private static final int Mapped_Order_Vol = 2;

    //    private String[] orderArr;
    private int orderNo;
    private int orderPrice;
    private int orderVol;

    Order(String[] orderArr) {
        orderNo = orderArr[Order_No].hashCode();
        Float price = Float.parseFloat(orderArr[Order_Price]) * 100;
        orderPrice = price.intValue();
        Float interOrderVol = Float.parseFloat(orderArr[Order_Vol]);
        orderVol = interOrderVol.intValue();
    }

    Order(String orderNo, String orderPrice, String orderVol) {
        this.orderNo = orderNo.hashCode();
        Float price = Float.parseFloat(orderPrice) * 100;
        this.orderPrice = price.intValue();
        Float interOrderVol = Float.parseFloat(orderVol);
        this.orderVol = interOrderVol.intValue();
    }

    int getOrderNo() {
        return orderNo;
    }

    int getOrderPrice() {
        return orderPrice;
    }
    int getOrderVol() {
        return orderVol;
    }

    void updateOrder(int otherOrderVol) {
        orderVol = (this.getOrderVol() - otherOrderVol);
    }

    @Override
    public String toString() {
        return orderNo + "|" + orderPrice + "|" + orderVol;
    }
}