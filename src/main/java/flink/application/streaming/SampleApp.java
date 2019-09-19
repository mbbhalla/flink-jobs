package flink.application.streaming;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SampleApp {
    
    @SuppressWarnings("serial")
    public static void main(String[] args) throws Exception {
        
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        env.fromElements("BLAHHH1", "BLAHHH2")
            .addSink(new SinkFunction<String>() {
                @SuppressWarnings("rawtypes")
                public void invoke(String value, Context context) throws Exception {
                    log.info("Blah: {}", value);
                }
            });
        
        env.execute();
    }
}
