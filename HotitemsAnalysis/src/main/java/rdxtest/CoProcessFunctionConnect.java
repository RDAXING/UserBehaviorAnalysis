package rdxtest;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 项目：UserBehaviorAnalysis
 * 包名：rdxtest
 * 作者：rdx
 * 日期：2021/7/13 11:39
 * 描述：连接两条流，进行相同key的一个取sum
 */
public class CoProcessFunctionConnect {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KeyedStream<Tuple2<String, Integer>, String> source1 = getSocketSource("localhost", 6666, env);
        KeyedStream<Tuple2<String, Integer>, String> source2 = getSocketSource("localhost", 7777, env);
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = source1.connect(source2).process(new MyConnectProcessFunction());
        resultStream.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static KeyedStream<Tuple2<String, Integer>, String> getSocketSource(String ip , int port, StreamExecutionEnvironment env){
        env.setParallelism(1);
        DataStreamSource<String> dataSource = env.socketTextStream(ip, port);
        KeyedStream<Tuple2<String, Integer>, String> resSource = dataSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                if (StringUtils.isBlank(s) || s.split(",").length != 2) {
                    return null;
                }
                return new Tuple2<>(s.split(",")[0], Integer.parseInt(s.split(",")[1]));
            }
        }).keyBy(f -> f.f0);

        return  resSource;
    }



}

class MyConnectProcessFunction extends CoProcessFunction<Tuple2<String,Integer>, Tuple2<String,Integer>, Tuple2<String,Integer>>{

    private transient ValueState<Integer> sum1State;
    private transient ValueState<Integer> sum2State;
    private transient ValueState<Long> tsState;
    private transient ValueState<String> currentKeyState;

    @Override
    public void open(Configuration parameters) throws Exception {
        sum1State = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("1state",Integer.class));
        sum2State = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("2.state",Integer.class));
        tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timeState",Long.class));
        currentKeyState = getRuntimeContext().getState(new ValueStateDescriptor<String>("currentKeyState",String.class));
    }

    @Override
    public void processElement1(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
        Integer current1Num = sum2State.value();
        if(current1Num == null){
            System.out.println("未接受到2流数据，将1号流数据存放到状态中");
            sum1State.update(value.f1);
            System.out.println("若在10s内未接受到2流数据，则测流输出延迟数据");
            long ts = ctx.timerService().currentProcessingTime() + 10000;
            ctx.timerService().registerProcessingTimeTimer(ts);
            tsState.update(ts);
            currentKeyState.update(value.f0);
        }else{
            System.out.println("接受到2流数据，将1号流数据与2号流数据进行相加");
            value.f1 += current1Num;
            out.collect(new Tuple2<>(value.f0,value.f1));
            ctx.timerService().deleteProcessingTimeTimer(tsState.value());
            tsState.clear();
            sum2State.clear();
            currentKeyState.clear();
        }
    }

    @Override
    public void processElement2(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
        Integer current1Num = sum1State.value();
        if(current1Num == null){
            System.out.println("未接受到1流数据，将2号流数据存放到状态中");
            sum2State.update(value.f1);
            System.out.println("若10s未接受到1号流的数据，则将数据进行测流输出到延迟数据");
            long ts = ctx.timerService().currentProcessingTime() + 10000;
            tsState.update(ts);
            ctx.timerService().registerProcessingTimeTimer(ts);
            currentKeyState.update(value.f0);
        }else{
            System.out.println("接受到1流数据，将2号流数据与2号流数据进行相加");
            value.f1 += current1Num;
            out.collect(new Tuple2<>(value.f0,value.f1));
            ctx.timerService().deleteProcessingTimeTimer(tsState.value());
            sum1State.clear();
            tsState.clear();
            currentKeyState.clear();
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
        Integer sum1S = sum1State.value();
        Integer sum2S = sum2State.value();
        if(sum1S!=null){
            System.out.println(currentKeyState.value() + "超过10s没有接受到数据");
            sum1State.clear();
        }
        if(sum2S!=null){
            System.out.println(currentKeyState.value() + "超过10s没有接受到数据");
            sum2State.clear();
        }
        currentKeyState.clear();

    }

    @Override
    public void close() throws Exception {
        sum1State.clear();
        sum2State.clear();
    }
}


