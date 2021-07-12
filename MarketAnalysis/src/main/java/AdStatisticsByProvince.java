import com.rdxcc.functions.MyFunctions;
import com.rdxcc.pojo.AdClickEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;

/**
 * 项目：UserBehaviorAnalysis
 * 包名：PACKAGE_NAME
 * 作者：rdx
 * 日期：2021/7/9 10:23
 */
public class AdStatisticsByProvince {
    public static void main(String[] args) {
        OutputTag<Tuple3<Long,Long,String>> outputTag = new OutputTag<Tuple3<Long, Long, String>>("back-list"){};
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        URL resource = AdStatisticsByProvince.class.getResource("AdClickLog.csv");
        DataStreamSource<String> dataSource = env.readTextFile(resource.getPath(), "UTF-8");
//        DataStreamSource<String> dataSource = env.socketTextStream("localhost",6666);
        SingleOutputStreamOperator<AdClickEvent> processSource = dataSource.map(new MapFunction<String, AdClickEvent>() {
            @Override
            public AdClickEvent map(String s) throws Exception {
                String[] ss = s.split(",");

                return new AdClickEvent(Long.parseLong(ss[0]), Long.parseLong(ss[1]), ss[2], ss[3], Long.parseLong(ss[4]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<AdClickEvent>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<AdClickEvent>() {
            @Override
            public long extractTimestamp(AdClickEvent adClickEvent, long l) {
                return adClickEvent.getTs() * 1000l;
            }
        })).keyBy(new KeySelector<AdClickEvent, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> getKey(AdClickEvent adClickEvent) throws Exception {
                return new Tuple2<>(adClickEvent.getUserId(), adClickEvent.getAdId());
            }
        }).process(new MyFunctions.MyAdEventKeyedProcessFunction(5, outputTag));

        processSource.print("正常数据");
        processSource.getSideOutput(outputTag).print("黑名单");

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
