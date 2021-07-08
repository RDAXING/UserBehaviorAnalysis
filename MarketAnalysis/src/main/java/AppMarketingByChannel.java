import com.rdxcc.functions.MyFunctions;
import com.rdxcc.functions.SimulatedMarketingBehaviorSource;
import com.rdxcc.pojo.ChannelPromotionCount;
import com.rdxcc.pojo.MarketingUserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 项目：UserBehaviorAnalysis
 * 包名：PACKAGE_NAME
 * 作者：rdx
 * 日期：2021/7/8 10:48
 */
public class AppMarketingByChannel {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<MarketingUserBehavior> dataSource = env.addSource(new SimulatedMarketingBehaviorSource());

//        dataSource.print("source");
        DataStream<ChannelPromotionCount> resultSource = dataSource.assignTimestampsAndWatermarks(WatermarkStrategy.<MarketingUserBehavior>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<MarketingUserBehavior>() {
            @Override
            public long extractTimestamp(MarketingUserBehavior marketingUserBehavior, long l) {
                return marketingUserBehavior.getTs();
            }
        })).filter(f -> !f.getBehavior().equals("UNINSTALL"))
                .keyBy(new KeySelector<MarketingUserBehavior, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(MarketingUserBehavior marketingUserBehavior) throws Exception {
                        return new Tuple2<>(marketingUserBehavior.getChannel(), marketingUserBehavior.getBehavior());
                    }
                })
                .timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new MyFunctions.MyAggregateFunction(), new MyFunctions.MyProcessWindowFunction());

        resultSource.print("resultData");
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
