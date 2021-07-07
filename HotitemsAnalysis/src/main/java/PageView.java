import com.google.common.collect.Lists;
import com.rdxcc.hotitemsanalysis.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 项目：UserBehaviorAnalysis
 * 包名：PACKAGE_NAME
 * 作者：rdx
 * 日期：2021/7/7 10:07
 *  961467,4904687,2735466,pv,1511678778
 */
public class PageView {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        URL resource = PageView.class.getResource("UserBehavior.csv");
        DataStreamSource<String> dataSource = env.readTextFile(resource.getPath(), "UTF-8");
        SingleOutputStreamOperator<Tuple2<String, Long>> pvSource = dataSource.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String s) throws Exception {
                String[] ss = s.split(",");
                return new UserBehavior(Long.parseLong(ss[0]), Long.parseLong(ss[1]), Integer.parseInt(ss[2]), ss[3], Long.parseLong(ss[4]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
            @Override
            public long extractTimestamp(UserBehavior userBehavior, long l) {
                return userBehavior.getTimestamp() * 1000l;
            }
        })).filter(f -> f.getBehavior().equals("pv"))
                .map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(UserBehavior userBehavior) throws Exception {
                        return new Tuple2<>(userBehavior.getBehavior(), 1l);
                    }
                }).keyBy(f -> f.f0).window(TumblingEventTimeWindows.of(Time.minutes(10)))
//                .sum(1)//只是进行每个窗口数据的统计：(pv,13)
                .process(new ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Tuple2<String, Long>> out) throws Exception {
                        int size =  Lists.newArrayList(elements).size();
                        long start = context.window().getStart();
                        long end = context.window().getEnd();

                        out.collect(new Tuple2<>("当前窗口区间："+ getDate(start,end) + "  " + s,size+0l));
                    }
                })//会打印出相关的窗口信息：(当前窗口区间：[2017-11-26 18:00:00--2017-11-26 18:10:00]  pv,13)
                ;

        pvSource.print("pageView");
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    public static String getDate(Long start,Long end){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String startD = sdf.format(new Date(start));
        String endD = sdf.format(new Date(end));
        return "["+startD +"--"+endD+"]";
    }
}
