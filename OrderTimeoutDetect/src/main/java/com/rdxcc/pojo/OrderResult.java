package com.rdxcc.pojo;

/**
 * 项目：UserBehaviorAnalysis
 * 包名：com.rdxcc.pojo
 * 作者：rdx
 * 日期：2021/7/12 11:02
 */
public class OrderResult {
    private Long orderId;
    private String payId;
    private String result;

    public OrderResult(Long orderId, String payId, String result) {
        this.orderId = orderId;
        this.payId = payId;
        this.result = result;
    }

    public OrderResult() {
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getPayId() {
        return payId;
    }

    public void setPayId(String payId) {
        this.payId = payId;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    @Override
    public String toString() {
        return "OrderResult{" +
                "orderId=" + orderId +
                ", payId='" + payId + '\'' +
                ", result='" + result + '\'' +
                '}';
    }
}
