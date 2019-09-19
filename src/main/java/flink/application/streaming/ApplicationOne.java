package flink.application.streaming;

import java.time.Instant;
import java.util.Date;
import java.util.stream.StreamSupport;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;

import lombok.Builder;
import lombok.Getter;
import lombok.val;

/*
 * Keys tweets by language and finds tweet count in 1 minute window (based on ingestion time)
 */
public class ApplicationOne {

    @Builder
    @Getter
    static class Tweet {
        private Instant timestamp;
        private String lang;
    }
    
    @SuppressWarnings("serial")
    public static void main(String[] args) throws Exception {
        
        final ObjectMapper mapper = new ObjectMapper();
        
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        
        val props = KinesisAnalyticsRuntime.getApplicationProperties()
            .get("configuration");
        
        val dataStream = env.addSource(new TwitterSource(props))
            .filter(tweetJsonString -> tweetJsonString.contains("\"created_at\""))
            .map(tweetJsonString -> { 
                JsonNode jsonNode = mapper.readTree(tweetJsonString);
                return Tweet.builder()
                    .lang(jsonNode.get("lang").asText())
                    .timestamp(Instant.ofEpochMilli(jsonNode.get("timestamp_ms").asLong()))
                    .build();
            })
            .keyBy(tweetObject -> tweetObject.getLang())
            .timeWindow(Time.seconds(2))
            .apply(new WindowFunction<Tweet, Tuple3<String, Long, Date>, String, TimeWindow>() {
                @Override
                public void apply(String lang, 
                    TimeWindow window, 
                    Iterable<Tweet> input, 
                    Collector<Tuple3<String, Long, Date>> out) throws Exception {

                    out.collect(Tuple3.of(
                        lang, 
                        StreamSupport.stream(input.spliterator(), true).count(), 
                        Date.from(Instant.ofEpochMilli(window.getEnd()))
                    ));
                }
            });
        
        dataStream.print();
        
        env.execute("Twitter keyBy Lang window count");
    }
}
