import com.rdxcc.com.rdxcc.utils.Configs;
import com.rdxcc.functions.ItermFunctions;
import com.rdxcc.hotitemsanalysis.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * 项目：UserBehaviorAnalysis
 * 包名：PACKAGE_NAME
 * 作者：rdx
 * 日期：2021/7/2 13:31
 */
public class HotItems {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        String topic = "test";
        String groupId = "group_fk";
        //读取不同的数据源哈哈
        String path = "D:\\ideaworkspace\\UserBehaviorAnalysis\\HotitemsAnalysis\\src\\main\\resources\\UserBehavior.csv";
        //获取文件数据
        //        DataStreamSource<String> dataSource = env.readTextFile(path, "UTF-8");
        //获取kafka数据
//        DataStreamSource<String> dataSource = env.addSource(new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), Configs.getKafkaConfig(groupId)));
        //获取socket流测试实时数据的获取
        DataStreamSource<String> dataSource = env.socketTextStream("localhost",6666);
        dataSource.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String s) throws Exception {
                String[] value = s.split(",");

                return new UserBehavior(Long.parseLong(value[0]), Long.parseLong(value[1]), Integer.parseInt(value[2]), value[3], Long.parseLong(value[4]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
            @Override
            public long extractTimestamp(UserBehavior userBehavior, long l) {
                return userBehavior.getTimestamp() * 1000;
            }
        })).uid("map-id")
                .filter(new FilterFunction<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior userBehavior) throws Exception {
                        if (userBehavior.getBehavior().equals("pv")) {
                            return true;
                        } else {

                            return false;
                        }
                    }
                })
                .keyBy(f -> f.getItemId())
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new ItermFunctions.MyHotItemAggregateFunction(), new ItermFunctions.MyHotItemProcessWindowFunction()).uid("aggregateId")
                .keyBy(f ->f.getWindowEnd())
                .process(new ItermFunctions.MyHotItemTopNKeyedProcessFunction(5))
                .print("Top");


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
