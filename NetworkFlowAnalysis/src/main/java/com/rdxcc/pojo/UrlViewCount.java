package com.rdxcc.pojo;

/**
 * 项目：UserBehaviorAnalysis
 * 包名：com.rdxcc.pojo
 * 作者：rdx
 * 日期：2021/7/6 14:43
 */
public class UrlViewCount {
    private String url;
    private Long endWindow;
    private Long count;

    public UrlViewCount() {
    }

    public UrlViewCount(String url, Long endWindow, Long count) {
        this.url = url;
        this.endWindow = endWindow;
        this.count = count;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getEndWindow() {
        return endWindow;
    }

    public void setEndWindow(Long endWindow) {
        this.endWindow = endWindow;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "UrlViewCount{" +
                "url='" + url + '\'' +
                ", endWindow=" + endWindow +
                ", count=" + count +
                '}';
    }
}
