import com.rdxcc.functions.NetWorkFunction;
import com.rdxcc.pojo.ApacheLogEvent;
import com.rdxcc.pojo.UrlViewCount;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.Test;

import java.time.Duration;
import java.util.regex.Pattern;

/**
 * 项目：UserBehaviorAnalysis
 * 包名：PACKAGE_NAME
 * 作者：rdx
 * 日期：2021/7/6 14:46
 */
public class NetworkFlow {
    public static void main(String[] args) {
        OutputTag<ApacheLogEvent> lateTag = new OutputTag<ApacheLogEvent>("late") {
        };
        String path = "D:\\ideaworkspace\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> dataSouce = env.readTextFile(path, "UTF-8");
        SingleOutputStreamOperator<ApacheLogEvent> waterMarkSource = dataSouce.map(new MapFunction<String, ApacheLogEvent>() {
            @Override
            public ApacheLogEvent map(String s) throws Exception {
                String[] ss = s.split(" ");
                FastDateFormat fdf = FastDateFormat.getInstance("dd/MM/yyyy:HH:mm:ss");
                long currentDate = fdf.parse(ss[3]).getTime();
                return new ApacheLogEvent(ss[0], currentDate, ss[4], ss[5], ss[6]);
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<ApacheLogEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(new SerializableTimestampAssigner<ApacheLogEvent>() {
            @Override
            public long extractTimestamp(ApacheLogEvent apacheLogEvent, long l) {
                return apacheLogEvent.getVisitDate();
            }
        }));

        SingleOutputStreamOperator<String> resultSource = waterMarkSource.filter(data -> "GET".equals(data.getRequest()))
                .filter(data -> {
                    String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                    return Pattern.matches(regex, data.getUrl());
                }).keyBy(f -> f.getUrl())
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(5)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(lateTag)
                .aggregate(new NetWorkFunction.NetWorkAggregate(), new NetWorkFunction.PageCountResult())
                .keyBy(f -> f.getEndWindow())
                .process(new NetWorkFunction.PageTopNFunction(3));

        resultSource.print("result");
        resultSource.getSideOutput(lateTag).print("late");


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test11(){
//        String log = "83.149.9.216 - - 17/05/2015:10:05:03 +0000 GET /presentations/logstash-monitorama-2013/images/kibana-search.png";
//        String[] split = log.split(" ");
//        for(int i=0;i<split.length;i++){
//            System.out.println("index:" + i + ",value :" + split[i]);
//        }
        String url = "/blog/geekery/bypassing-captive-portals.html";
//        String url = "/presentations/logstash-monitorama-2013/images/kibana-search.png";
        String regex = "^((?!\\.(css|js|png|ico)$).)*$";
        boolean matches = Pattern.matches(regex, url);
        System.out.println(matches);
    }
}
