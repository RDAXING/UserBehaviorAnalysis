package com.rdxcc.functions;

import com.rdxcc.pojo.MarketingUserBehavior;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * 项目：UserBehaviorAnalysis
 * 包名：com.rdxcc.functions
 * 作者：rdx
 * 日期：2021/7/8 10:31
 */
public class SimulatedMarketingBehaviorSource implements SourceFunction<MarketingUserBehavior> {

    boolean running = true;
    List<String> behaviorList = Arrays.asList("CLICK", "DOWNLOAD", "INSTALL","UNINSTALL");
    List<String> channelList = Arrays.asList("app store", "weibo", "wechat","tieba");
    Random random = new Random();

    @Override
    public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
        while(running){
            String behavior = behaviorList.get(random.nextInt(behaviorList.size()));
            String channel = channelList.get(random.nextInt(channelList.size()));
            long id = Math.abs(random.nextLong()-(behavior.hashCode()+channel.hashCode()));
            long ts = System.currentTimeMillis();
            ctx.collect(new MarketingUserBehavior(id,behavior,channel,ts));
            Thread.sleep(500);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
