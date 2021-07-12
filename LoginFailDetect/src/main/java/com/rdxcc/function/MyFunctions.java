package com.rdxcc.function;

import com.google.common.collect.Lists;
import com.rdxcc.pojo.LoginEvent;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 项目：UserBehaviorAnalysis
 * 包名：com.rdxcc.function
 * 作者：rdx
 * 日期：2021/7/9 14:03
 */
public class MyFunctions {
    public static class MyPatternSelectFunction implements PatternSelectFunction<LoginEvent, Tuple4<Long,Long,Long,String>>{

        @Override
        public Tuple4<Long, Long, Long, String> select(Map<String, List<LoginEvent>> map) throws Exception {
            LoginEvent start = map.get("start").iterator().next();
            LoginEvent second = map.get("second").iterator().next();

            return new Tuple4<>(start.getId(),start.getTs(),second.getTs(),"2s内连续两次登录失败");
        }
    }

    public static class MyLoginFailProcessFunction extends KeyedProcessFunction<Long, LoginEvent, Tuple4<Long,Long,Long,String>>{

        private transient ValueState<Long> timerState;
        private transient ListState<LoginEvent> loginEventListState;
        @Override
        public void open(Configuration parameters) throws Exception {
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerState", TypeInformation.of(new TypeHint<Long>() {
            })));
            loginEventListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("loginEventListState",TypeInformation.of(new TypeHint<LoginEvent>() {
            })));
        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<Tuple4<Long, Long, Long, String>> out) throws Exception {
            Long times = timerState.value();
            if("fail".equals(value.getType())){
                loginEventListState.add(value);
                if(null == times){
                    long timeTs = value.getTs() + 2l;
                    ctx.timerService().registerEventTimeTimer(timeTs);
                    timerState.update(timeTs);
                }
            }else{
                if(timerState.value() != null){

                    ctx.timerService().deleteEventTimeTimer(timerState.value());
                }
                timerState.clear();
                loginEventListState.clear();
            }

        }


        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple4<Long, Long, Long, String>> out) throws Exception {
            ArrayList<LoginEvent> loginEvents = Lists.newArrayList(loginEventListState.get().iterator());
            if(loginEvents.size()>=2){
                out.collect(new Tuple4<>(ctx.getCurrentKey(),loginEvents.get(0).getTs(),loginEvents.get(loginEvents.size()-1).getTs(),"连续两次登录失败"));
            }

            timerState.clear();
            loginEventListState.clear();
        }
    }

    public static class MyLoginNotStateProcessFunction extends KeyedProcessFunction<Long, LoginEvent, Tuple4<Long,Long,Long,String>>{

        private transient ValueState<LoginEvent> loginEventValueState ;

        @Override
        public void open(Configuration parameters) throws Exception {
            loginEventValueState = getRuntimeContext().getState(new ValueStateDescriptor<LoginEvent>("login fail",TypeInformation.of(new TypeHint<LoginEvent>() {
           })));
        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<Tuple4<Long, Long, Long, String>> out) throws Exception {
            if(value.getType().equals("fail")){
                LoginEvent firstValue = loginEventValueState.value();
                if(null != firstValue && value.getTs()-firstValue.getTs() <=2){
                    out.collect(new Tuple4<>(ctx.getCurrentKey(),firstValue.getTs(),value.getTs(),"login fail 2"));
                    loginEventValueState.clear();
                    loginEventValueState.update(value);
                }else{
                    loginEventValueState.update(value);
                }
            }else{
                loginEventValueState.clear();
            }
        }

        @Override
        public void close() throws Exception {
            loginEventValueState.clear();
        }
    }

}
