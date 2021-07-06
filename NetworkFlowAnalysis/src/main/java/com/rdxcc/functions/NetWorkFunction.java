package com.rdxcc.functions;

import com.google.common.collect.Lists;
import com.rdxcc.pojo.ApacheLogEvent;
import com.rdxcc.pojo.UrlViewCount;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 项目：UserBehaviorAnalysis
 * 包名：com.rdxcc.functions
 * 作者：rdx
 * 日期：2021/7/6 15:31
 */
public class NetWorkFunction {
    public static class NetWorkAggregate implements AggregateFunction<ApacheLogEvent,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0l;
        }

        @Override
        public Long add(ApacheLogEvent apacheLogEvent, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return acc1+aLong;
        }
    }


    public static class PageCountResult implements WindowFunction<Long, UrlViewCount, String, TimeWindow>{

        @Override
        public void apply(String url, TimeWindow window, Iterable<Long> input, Collector<UrlViewCount> out) throws Exception {
            long endWindowTime = window.getEnd();
            Long count = input.iterator().next();
            out.collect(new UrlViewCount(url,endWindowTime,count));
        }
    }


    public static class PageTopNFunction extends KeyedProcessFunction<Long, UrlViewCount, String>{

        private Integer topN;
        private MapState<String,Long> pageUrlCountState;
        public PageTopNFunction(Integer topN) {
            this.topN = topN;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            pageUrlCountState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("page-count-map",String.class,Long.class));
        }

        @Override
        public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {
            pageUrlCountState.put(value.getUrl(),value.getCount());
            ctx.timerService().registerEventTimeTimer(value.getEndWindow()+1);
//            ctx.timerService().registerEventTimeTimer(value.getEndWindow()+60*1000);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            Iterator<Map.Entry<String, Long>> it = pageUrlCountState.entries().iterator();
            ArrayList<Map.Entry<String, Long>> entries = Lists.newArrayList(it);
            entries.sort(new Comparator<Map.Entry<String, Long>>() {
                @Override
                public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                    if(o1.getValue()-o2.getValue() > 0){
                        return -1;
                    }else if(o1.getValue()-o2.getValue() < 0){
                        return 1;
                    }else
                    return 0;
                }
            });

            StringBuffer sbf = new StringBuffer();
            sbf.append("\n结束窗口时间：" +new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date((timestamp-1))) + "\n");
            for(int i=0;i<Math.min(topN,entries.size());i++){
                Map.Entry<String, Long> map = entries.get(i);
                sbf.append("Top" + (i+1) + "    url：" + map.getKey() + "    点击量："+map.getValue() +"\n");
            }
            Thread.sleep(500);
            out.collect(sbf.toString());
        }

        @Override
        public void close() throws Exception {
            pageUrlCountState.clear();
        }
    }

}
