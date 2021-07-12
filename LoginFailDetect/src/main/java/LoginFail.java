import com.rdxcc.function.MyFunctions;
import com.rdxcc.pojo.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 项目：UserBehaviorAnalysis
 * 包名：PACKAGE_NAME
 * 作者：rdx
 * 日期：2021/7/9 14:08
 */
public class LoginFail {
    public static void main(String[] args) {
        OutputTag<LoginEvent> outputTag = new OutputTag<LoginEvent>("late"){};
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        DataStreamSource<String> dataSource = env.readTextFile(LoginFail.class.getResource("LoginLog.csv").getPath(), "UTF-8");
        DataStreamSource<String> dataSource = env.socketTextStream("localhost", 6666);
        SingleOutputStreamOperator<LoginEvent> processData = dataSource.map(new MapFunction<String, LoginEvent>() {
            @Override
            public LoginEvent map(String s) throws Exception {
                String[] ss = s.split(",");
                return new LoginEvent(Long.parseLong(ss[0]), ss[1], ss[2], Long.parseLong(ss[3]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
            @Override
            public long extractTimestamp(LoginEvent loginEvent, long l) {
                return loginEvent.getTs() * 1000l;
            }
        })).keyBy(f -> f.getId())
                .timeWindow(Time.seconds(5)).process(new ProcessWindowFunction<LoginEvent, LoginEvent, Long, TimeWindow>() {
                    @Override
                    public void process(Long aLong, Context context, Iterable<LoginEvent> elements, Collector<LoginEvent> out) throws Exception {
                        for (LoginEvent le : elements) {
                            out.collect(le);
                        }
                    }
                });
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("start").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent) throws Exception {
                if (loginEvent.getType().equals("fail")) {
                    return true;
                } else
                    return false;
            }
        }).next("second").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent) throws Exception {
                if (loginEvent.getType().equals("fail")) {
                    return true;
                } else
                    return false;
            }
        }).within(Time.seconds(2));

        
        
        PatternStream<LoginEvent> ps = CEP.pattern(processData, pattern);
        SingleOutputStreamOperator<Tuple4<Long, Long, Long, String>> select = ps.select(new MyFunctions.MyPatternSelectFunction());
        select.print("loginWarning");


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
