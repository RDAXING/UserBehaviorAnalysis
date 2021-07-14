package com.rdxcc.function;

import com.rdxcc.pojo.OrderEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 项目：UserBehaviorAnalysis
 * 包名：com.rdxcc.function
 * 作者：rdx
 * 日期：2021/7/12 16:07
 */
public class MyFunctions {
    public static class MyKeyedProcessFunction extends KeyedProcessFunction<Long, OrderEvent, Tuple3<Long,String,String>>{
        private transient ValueState<Boolean> isPayState;
        private transient ValueState<Boolean> isCreateState;
        private transient ValueState<Long> currentTsState;
        private OutputTag<OrderEvent> outputTag;

        public MyKeyedProcessFunction(OutputTag<OrderEvent> outputTag) {
            this.outputTag = outputTag;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            isCreateState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("createState", TypeInformation.of(new TypeHint<Boolean>() {
            })));
            isPayState  = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("payState",TypeInformation.of(new TypeHint<Boolean>() {
            })));
            currentTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("tsState",TypeInformation.of(new TypeHint<Long>() {
            })));
        }

        @Override
        public void processElement(OrderEvent value, Context ctx, Collector<Tuple3<Long, String, String>> out) throws Exception {

            Boolean payValue = isPayState.value();
            Boolean createValue = isCreateState.value();
            Long tsValue = currentTsState.value();
            if(null == payValue){
                payValue = false;
            }
            if(null == createValue){
                createValue = false;
            }
            if("create".equals(value.getPayType())){
                if(payValue){
                    out.collect(new Tuple3<>(value.getOrderId(),getDate(value.getPayTime()*1000L),"pay Success"));
                    ctx.timerService().deleteEventTimeTimer(tsValue);
                    isPayState.clear();
                    isCreateState.clear();
                    currentTsState.clear();
                }else{
                    long currentPayTime = value.getPayTime() * 1000L + 15 * 60 * 1000L;
                    ctx.timerService().registerEventTimeTimer(currentPayTime);
                    isCreateState.update(true);
                    currentTsState.update(currentPayTime);
                }
            }else if("pay".equals(value.getPayType())){
                if(createValue){
                    if(value.getPayTime()*1000L <= tsValue){
                        out.collect(new Tuple3<>(value.getOrderId(),getDate(value.getPayTime()),"pay Success"));

                    }else{
                        ctx.output(outputTag,value);
                    }
                    isCreateState.clear();
                    isPayState.clear();
                    currentTsState.clear();
                }else{
                    ctx.timerService().registerEventTimeTimer(value.getPayTime()*1000L);
                    isPayState.update(true);
                    currentTsState.update(value.getPayTime()*1000L);
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<Long, String, String>> out) throws Exception {
            Boolean isCreate = isCreateState.value();
            Boolean isPay = isPayState.value();
            if(isCreate.equals(false) && isPay.equals(true)){

            }
        }
    }


    public static class MyCoProcessFunction extends CoProcessFunction<OrderEvent, Tuple3<String,String,Long>, Tuple4<Long,String,String,String>>{
        private transient ValueState<OrderEvent> orderPayIdState;
        private transient ValueState<Tuple3<String,String,Long>> receiptPayIdState;
        private OutputTag<Tuple4<Long,String,String,String>> outputTag ;

        public MyCoProcessFunction(OutputTag<Tuple4<Long, String, String, String>> outputTag) {
            this.outputTag = outputTag;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            orderPayIdState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("order",TypeInformation.of(new TypeHint<OrderEvent>() {
            })));
            receiptPayIdState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple3<String,String,Long>>("receipt",TypeInformation.of(new TypeHint<Tuple3<String,String,Long>>() {
            })));
        }

        @Override
        public void processElement1(OrderEvent value, Context ctx, Collector<Tuple4<Long, String, String, String>> out) throws Exception {
            Tuple3<String, String, Long> receiptValue = receiptPayIdState.value();
            long ts = value.getPayTime() * 1000 + 5000;
            if(null == receiptValue){
                //未接受到receipt的数据，将数据保存到状态中
                orderPayIdState.update(value);
                //注册定时
                ctx.timerService().registerEventTimeTimer(ts);


            }else{
                out.collect(new Tuple4<>(value.getOrderId(),receiptValue.f1,getDate(value.getPayTime())+"--"+getDate(receiptValue.f2),"支付成功"));
//                orderPayIdState.clear();
                receiptPayIdState.clear();
                ctx.timerService().deleteEventTimeTimer(ts);
            }
        }

        @Override
        public void processElement2(Tuple3<String, String, Long> value, Context ctx, Collector<Tuple4<Long, String, String, String>> out) throws Exception {
            OrderEvent orderPayId = orderPayIdState.value();
            long ts = value.f2 + 1000L + 3000;
            if(null == orderPayId){
                receiptPayIdState.update(value);
                ctx.timerService().registerEventTimeTimer(ts);
            }else{
                out.collect(new Tuple4<>(orderPayId.getOrderId(),value.f1,getDate(orderPayId.getPayTime())+"--"+getDate(value.f2),"支付成功"));
//                receiptPayIdState.clear();
                orderPayIdState.clear();
                ctx.timerService().deleteEventTimeTimer(ts);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple4<Long, String, String, String>> out) throws Exception {
            OrderEvent orderEvent = orderPayIdState.value();
            Tuple3<String, String, Long> receiptValue = receiptPayIdState.value();
            if(orderEvent!=null){
                ctx.output(outputTag,new Tuple4<>(orderEvent.getOrderId(),orderEvent.getPayType(),"5s内未接受到receipt的数据:"+orderEvent.getPayId(),"fail"));
                orderPayIdState.clear();
            }

            if(receiptValue != null){
                ctx.output(outputTag,new Tuple4<>(0L,receiptValue.f1,"3s内未接受到orderLog对应payId的数据:"+receiptValue.f0,"fail"));
                receiptPayIdState.clear();
            }
        }
    }
    private static String getDate(Long startdate){
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
        String start = sdf.format(new Date(startdate*1000L));

        return "["+start+"]";
    }
}
