package za.co.simplitate.flink.p1;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCount {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> text = env.socketTextStream("localhost", 9999);
        DataStream<Tuple2<String, Integer>> counts = text.filter((FilterFunction<String>) s -> s.startsWith("N"))
                .map(new Tokenizer())
                .keyBy(0)
                .sum(1);
        counts.print();

        env.execute("Streaming Count");
    }

    public static final class Tokenizer implements MapFunction<String, Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> map(String value) {
            return new Tuple2<String, Integer>(value, 1);
        }
    }
}
