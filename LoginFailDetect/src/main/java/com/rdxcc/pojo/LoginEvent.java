package com.rdxcc.pojo;

/**
 * 项目：UserBehaviorAnalysis
 * 包名：com.rdxcc.pojo
 * 作者：rdx
 * 日期：2021/7/9 14:03
 */
public class LoginEvent {
    private Long id;
    private String ip;
    private String type;
    private Long ts;

    public LoginEvent(Long id, String ip, String type, Long ts) {
        this.id = id;
        this.ip = ip;
        this.type = type;
        this.ts = ts;
    }

    public LoginEvent() {
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "LoginEvent{" +
                "id=" + id +
                ", ip='" + ip + '\'' +
                ", type='" + type + '\'' +
                ", ts=" + ts +
                '}';
    }
}
