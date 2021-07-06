package com.rdxcc.functions;

import com.google.common.collect.Lists;
import com.rdxcc.hotitemsanalysis.ItemViewCount;
import com.rdxcc.hotitemsanalysis.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 项目：UserBehaviorAnalysis
 * 包名：com.rdxcc.functions
 * 作者：rdx
 * 日期：2021/7/2 13:53
 */
public class ItermFunctions {
    /**
     *
     */
    public static class MyHotItemAggregateFunction implements AggregateFunction<UserBehavior, Long, Long>{

        @Override
        public Long createAccumulator() {
            return 0l;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong+acc1;
        }
    }


    public static class MyHotItemProcessWindowFunction extends ProcessWindowFunction<Long, ItemViewCount, Long, TimeWindow>{

        @Override
        public void process(Long aLong, Context context, Iterable<Long> elements, Collector<ItemViewCount> out) throws Exception {
            long endWindow = context.window().getEnd();
            Long count = elements.iterator().next();
            out.collect(new ItemViewCount(aLong,endWindow,count));
        }
    }


    public static class MyHotItemTopNKeyedProcessFunction extends KeyedProcessFunction<Long, ItemViewCount,String>{

        private Integer interval;
        private transient  ListState<ItemViewCount> listItemsState;
        public MyHotItemTopNKeyedProcessFunction(Integer interval) {
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            listItemsState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("listItemsState", TypeInformation.of(new TypeHint<ItemViewCount>() {
            })));
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            listItemsState.add(value);
            ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey()+1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(listItemsState.get().iterator());
            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o2.getCount().intValue()-o1.getCount().intValue();
                }
            });

            StringBuffer sbf = new StringBuffer();
            for(int i=0;i<Math.min(interval,itemViewCounts.size());i++){
                ItemViewCount itemViewCount = itemViewCounts.get(i);
                sbf.append("No" + (i+1)).append(":")
                        .append(itemViewCount.getItemId())
                        .append("，点击量：")
                        .append(itemViewCount.getCount())
                        .append(", 窗口结束时间：")
                        .append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(itemViewCount.getWindowEnd())))
                        .append("\n");
            }

            Thread.sleep(500);
            out.collect(sbf.toString());
        }

        @Override
        public void close() throws Exception {
            listItemsState.clear();
        }
    }
}
