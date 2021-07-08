package com.rdxcc.pojo;

/**
 * 项目：UserBehaviorAnalysis
 * 包名：com.rdxcc.pojo
 * 作者：rdx
 * 日期：2021/7/8 10:32
 */
public class MarketingUserBehavior {
    private Long id;
    private String behavior;
    private String channel;
    private Long ts;

    public MarketingUserBehavior(Long id, String behavior, String channel, Long ts) {
        this.id = id;
        this.behavior = behavior;
        this.channel = channel;
        this.ts = ts;
    }

    public MarketingUserBehavior() {
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "MarketingUserBehavior{" +
                "id=" + id +
                ", behavior='" + behavior + '\'' +
                ", channel='" + channel + '\'' +
                ", ts=" + ts +
                '}';
    }
}
