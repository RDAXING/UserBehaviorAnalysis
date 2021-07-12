package com.rdxcc.pojo;

/**
 * 项目：UserBehaviorAnalysis
 * 包名：com.rdxcc.pojo
 * 作者：rdx
 * 日期：2021/7/9 9:47
 */
public class AdCountByProvince {
    private String province;
    private Long windowEnd;
    private Long count;

    public AdCountByProvince(String province, Long windowEnd, Long count) {
        this.province = province;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public AdCountByProvince() {
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "AdCountByProvince{" +
                "province='" + province + '\'' +
                ", windowEnd=" + windowEnd +
                ", count=" + count +
                '}';
    }
}
