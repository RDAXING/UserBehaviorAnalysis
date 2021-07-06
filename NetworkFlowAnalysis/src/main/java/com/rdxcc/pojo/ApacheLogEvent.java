package com.rdxcc.pojo;

import org.junit.Test;

import java.util.regex.Pattern;

/**
 * 项目：UserBehaviorAnalysis
 * 包名：com.rdxcc.pojo
 * 作者：rdx
 * 日期：2021/7/6 11:45
 */
public class ApacheLogEvent {
    //源数据字段
    //83.149.9.216 - - 17/05/2015:10:05:03 +0000 GET /presentations/logstash-monitorama-2013/images/kibana-search.png
    private String ip;
    private Long visitDate;
    private String other;
    private String request;
    private String url;


    public ApacheLogEvent() {
    }

    public ApacheLogEvent(String ip, Long visitDate, String other, String request, String url) {
        this.ip = ip;
        this.visitDate = visitDate;
        this.other = other;
        this.request = request;
        this.url = url;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Long getVisitDate() {
        return visitDate;
    }

    public void setVisitDate(Long visitDate) {
        this.visitDate = visitDate;
    }

    public String getOther() {
        return other;
    }

    public void setOther(String other) {
        this.other = other;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public String toString() {
        return "ApacheLogEvent{" +
                "ip='" + ip + '\'' +
                ", visitDate=" + visitDate +
                ", other='" + other + '\'' +
                ", request='" + request + '\'' +
                ", url='" + url + '\'' +
                '}';
    }


}
