package com.rdxcc.functions;

import com.rdxcc.pojo.AdClickEvent;
import com.rdxcc.pojo.ChannelPromotionCount;
import com.rdxcc.pojo.MarketingUserBehavior;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

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


    public static class MyAdEventKeyedProcessFunction extends KeyedProcessFunction<Tuple2<Long,Long>, AdClickEvent, AdClickEvent>{

        private Integer limitCount;
        private OutputTag<Tuple3<Long,Long,String>> outputTag;
        private transient ValueState<Integer> countState;
        private transient ValueState<Boolean> addBackListState;

        public MyAdEventKeyedProcessFunction(Integer limitCount, OutputTag<Tuple3<Long, Long, String>> outputTag) {
            this.limitCount = limitCount;
            this.outputTag = outputTag;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("countState", TypeInformation.of(new TypeHint<Integer>() {
            })));
            addBackListState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-sent",TypeInformation.of(new TypeHint<Boolean>() {
            })));
        }

        @Override
        public void processElement(AdClickEvent value, Context ctx, Collector<AdClickEvent> out) throws Exception {
            Integer currentCount = countState.value();

            if(currentCount == null ){
                currentCount = 0;
            }
            long ts = ctx.timerService().currentWatermark() + 5000;
            ctx.timerService().registerEventTimeTimer(ts);
            countState.update(currentCount+1);
            if(currentCount >= limitCount){
                ctx.output(outputTag,new Tuple3<>(value.getUserId(),value.getAdId(),"数量:"+currentCount+",存在刷单行为，加入黑名单"));
                countState.clear();
            }else {
                out.collect(value);
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
            countState.clear();
        }

        @Override
        public void close() throws Exception {
            countState.clear();
            addBackListState.clear();
        }
    }

}
