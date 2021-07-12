import com.google.common.collect.Lists;
import com.rdxcc.function.MyFunctions;
import com.rdxcc.pojo.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;

/**
 * 项目：UserBehaviorAnalysis
 * 包名：PACKAGE_NAME
 * 作者：rdx
 * 日期：2021/7/9 16:08
 */
public class LoginStateFail {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> dataSource = env.readTextFile(LoginStateFail.class.getResource("LoginLog.csv").getPath(), "UTF-8");
        dataSource.map(new MapFunction<String, LoginEvent>() {
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
//                .process(new MyFunctions.MyLoginFailProcessFunction()).print("2fail");
                .process(new MyFunctions.MyLoginNotStateProcessFunction()).print("not state2");
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
