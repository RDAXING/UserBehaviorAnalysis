package com.rdxcc.pojo;

/**
 * 项目：UserBehaviorAnalysis
 * 包名：com.rdxcc.pojo
 * 作者：rdx
 * 日期：2021/7/12 11:01
 */
public class OrderEvent {
    private Long orderId;
    private String payType;
    private String payId;
    private Long payTime;

    public OrderEvent(Long orderId, String payType, String payId, Long payTime) {
        this.orderId = orderId;
        this.payType = payType;
        this.payId = payId;
        this.payTime = payTime;
    }

    public OrderEvent() {
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getPayType() {
        return payType;
    }

    public void setPayType(String payType) {
        this.payType = payType;
    }

    public String getPayId() {
        return payId;
    }

    public void setPayId(String payId) {
        this.payId = payId;
    }

    public Long getPayTime() {
        return payTime;
    }

    public void setPayTime(Long payTime) {
        this.payTime = payTime;
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderId=" + orderId +
                ", payType='" + payType + '\'' +
                ", payId='" + payId + '\'' +
                ", payTime=" + payTime +
                '}';
    }
}
