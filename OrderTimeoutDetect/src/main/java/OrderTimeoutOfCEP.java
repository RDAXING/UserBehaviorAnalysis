import com.rdxcc.function.MyFunctions;
import com.rdxcc.pojo.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * 项目：UserBehaviorAnalysis
 * 包名：PACKAGE_NAME
 * 作者：rdx
 * 日期：2021/7/12 11:09
 * 通过复杂事件处理订单超时
 */
public class OrderTimeoutOfCEP {

    public static void main(String[] args) {
        OutputTag<Tuple4<Long,String,String,String>> outputTag1 = new OutputTag<Tuple4<Long, String, String, String>>("timeOut"){};
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        DataStreamSource<String> dataSource = env.readTextFile(OrderTimeoutOfCEP.class.getResource("OrderLog.csv").getPath(), "UTF-8");
        DataStreamSource<String> dataSource = env.socketTextStream("localhost", 6666);
        SingleOutputStreamOperator<OrderEvent> mapSource = dataSource.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String s) throws Exception {
                String[] ss = s.split(",");

                return new OrderEvent(Long.parseLong(ss[0]), ss[1], ss[2], Long.parseLong(ss[3]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
            @Override
            public long extractTimestamp(OrderEvent orderEvent, long l) {
                return orderEvent.getPayTime() * 1000L;
            }
        }));

//        mapSource.print("source");
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("first")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        if ("create".equals(orderEvent.getPayType())) {
                            return true;
                        } else
                            return false;
                    }
                }).followedBy("second")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        if ("pay".equals(orderEvent.getPayType())) {
                            return true;
                        } else
                            return false;
                    }
                }).within(Time.minutes(15L));

        OutputTag<Tuple3<Long,String,String>> outputTag = new OutputTag<Tuple3<Long, String, String>>("timeout"){};

        SingleOutputStreamOperator<OrderEvent> selectSouce = CEP.pattern(mapSource.keyBy(f -> f.getOrderId()), pattern)
                .select(outputTag, new PatternTimeoutFunction<OrderEvent, Tuple3<Long, String, String>>() {
                    @Override
                    public Tuple3<Long, String, String> timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {
                        OrderEvent first = map.get("first").iterator().next();
                        return new Tuple3<>(first.getOrderId(), getDate(first.getPayTime()*1000L), "PayTimeOut");
                    }
                }, new PatternSelectFunction<OrderEvent, OrderEvent>() {
                    @Override
                    public OrderEvent select(Map<String, List<OrderEvent>> map) throws Exception {
                        return map.get("second").iterator().next();
                    }
                });


//        selectSouce.print("success");//未超时的数据流
//        selectSouce.getSideOutput(outputTag).print("timeOut");//超时数据的数据流
        SingleOutputStreamOperator<Tuple4<Long, String, String, String>> finallSource = selectSouce.keyBy(f -> f.getPayId()).connect(getReceiptLogData(env).keyBy(f -> f.f0))
                .process(new MyFunctions.MyCoProcessFunction(outputTag1));

        finallSource.print("coprocess");
        finallSource.getSideOutput(outputTag1).print("支付超时");
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    private static String getDate(Long startdate){
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
        String start = sdf.format(new Date(startdate));

        return "["+start+"]";
    }

    private static SingleOutputStreamOperator<Tuple3<String, String, Long>> getReceiptLogData(StreamExecutionEnvironment env){
//        DataStreamSource<String> dataSource = env.readTextFile(OrderTimeoutOfCEP.class.getResource("ReceiptLog.csv").getPath(), "UTF-8");
        DataStreamSource<String> dataSource = env.socketTextStream("localhost",7777);
        SingleOutputStreamOperator<Tuple3<String, String, Long>> resultData = dataSource.map(new MapFunction<String, Tuple3<String, String, Long>>() {
            @Override
            public Tuple3<String, String, Long> map(String s) throws Exception {
                String[] line = s.split(",");
                return new Tuple3<>(line[0], line[1], Long.parseLong(line[2]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
            @Override
            public long extractTimestamp(Tuple3<String, String, Long> t3, long l) {
                return t3.f2 * 1000L;
            }
        }));
        return resultData;
    }
}
