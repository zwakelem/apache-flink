package za.co.simplitate.flink.p1;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AverageProfit {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> stream = env.readTextFile("/Users/zmgabhi/Documents/flink/avg");
        DataStream<Tuple5<String, String, String, Integer, Integer>> mapped = stream.map(new Splitter());
        DataStream<Tuple5<String, String, String, Integer, Integer>> reduced = mapped.keyBy(0).reduce(new Reduce1());
        DataStream<Tuple2<String, Double>> profitPerMonth = reduced.map(new CalcProfits());

        profitPerMonth.print();
        env.execute("Avg Profits per months");
    }

    public static final class CalcProfits implements MapFunction<Tuple5<String, String, String, Integer, Integer>,
                                                                                        Tuple2<String, Double>> {

        @Override
        public Tuple2<String, Double> map(Tuple5<String, String, String, Integer, Integer> input)
                                                                                                    throws Exception {
            return new Tuple2<String, Double>(input.f0, new Double((input.f3 * 1.0) / input.f4));
        }
    }

    public static final class Splitter implements MapFunction<String, Tuple5<String, String, String, Integer, Integer>> {
        @Override
        public Tuple5<String, String, String, Integer, Integer> map(String s) throws Exception {
            String[] split = s.split(",");
            return new Tuple5<>(split[1], split[2], split[3], Integer.valueOf(split[4]), 1);
        }
    }

    public static final class Reduce1 implements ReduceFunction<Tuple5<String, String, String, Integer, Integer>> {
        // NOTE: for a reduce function, input and output should be of the same type
        // the FoldFunction is deprecated, its similar to reduce, but can return a different type from input
        @Override
        public Tuple5<String, String, String, Integer, Integer> reduce(Tuple5<String, String, String, Integer, Integer> curr,
                                                                       Tuple5<String, String, String, Integer, Integer> result) throws Exception {
            return new Tuple5<>(curr.f0, curr.f1, curr.f2, curr.f3 + result.f3, curr.f4 + result.f4);
        }
    }
}
