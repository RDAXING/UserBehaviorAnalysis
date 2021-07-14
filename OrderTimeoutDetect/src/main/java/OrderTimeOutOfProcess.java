import com.rdxcc.function.MyFunctions;
import com.rdxcc.pojo.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 项目：UserBehaviorAnalysis
 * 包名：PACKAGE_NAME
 * 作者：rdx
 * 日期：2021/7/12 15:52
 * 描述：通过processFunction 来处理支付超时情况
 */
public class OrderTimeOutOfProcess {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> dataSource = env.readTextFile(OrderTimeOutOfProcess.class.getResource("OrderLog.csv").getPath(), "UTF-8");
        dataSource.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String s) throws Exception {
                String[] ss = s.split(",");

                return new OrderEvent(Long.parseLong(ss[0]),ss[1],ss[2],Long.parseLong(ss[3]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
            @Override
            public long extractTimestamp(OrderEvent orderEvent, long l) {
                return orderEvent.getPayTime();
            }
        })).keyBy(f->f.getOrderId())
                .process(new MyFunctions.MyKeyedProcessFunction(new OutputTag<OrderEvent>("tag"){}));
    }
}
