package com.rdxcc.functions;

import com.rdxcc.pojo.ChannelPromotionCount;
import com.rdxcc.pojo.MarketingUserBehavior;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 项目：UserBehaviorAnalysis
 * 包名：com.rdxcc.functions
 * 作者：rdx
 * 日期：2021/7/8 17:55
 */
public class MyFunctions {
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    public static class MyAggregateFunction implements AggregateFunction<MarketingUserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0l;
        }

        @Override
        public Long add(MarketingUserBehavior marketingUserBehavior, Long accumulate) {
            return accumulate + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }


    public static class MyProcessWindowFunction extends ProcessWindowFunction<Long, ChannelPromotionCount, Tuple2<String,String>, TimeWindow>{

        @Override
        public void process(Tuple2<String, String> key, Context context, Iterable<Long> elements, Collector<ChannelPromotionCount> out) throws Exception {
            long endWindow = context.window().getEnd();
            String behavior = key.f0;
            String channel = key.f1;
            Long count = elements.iterator().next();
            out.collect(new ChannelPromotionCount(behavior,channel,sdf.format(new Date(endWindow)),count));
        }
    }
}
