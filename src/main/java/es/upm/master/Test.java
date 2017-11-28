package es.upm.master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import scala.collection.generic.BitOperations.Int;

public class Test {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<String> s2 = env.readTextFile("/Users/valerio/work/developing/gitlab/master-eit-flink/data/in.txt");

        SingleOutputStreamOperator<Tuple2<String, Integer>> s3 = s2.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String input, Collector<Tuple2<String, Integer>> collecotor) throws Exception {
                String[] wordArray = input.split(" ");
                //THIS CAN BE REPLACED BY SUM UP COMMON WORDS IN A LINE
                //AVOIDINF SENDING MORE THAN ONE OUTPUT TUPLE PER WORD
                for (String word : wordArray) {
                    Tuple2<String, Integer> t = new Tuple2<>(word,1);
                    collecotor.collect(t);
                }
            }

        });

        KeyedStream<Tuple2<String, Integer>, Tuple> s4 = s3.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> s5 = s4.reduce(new ReduceFunction<Tuple2<String,Integer>>() {

            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> accumulator, Tuple2<String, Integer> input) throws Exception {

                Tuple2<String, Integer> out = new Tuple2<>(accumulator.f0, accumulator.f1 + input.f1);
                return out;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> s6 = s5.filter(new FilterFunction<Tuple2<String,Integer>>() {

            @Override
            public boolean filter(Tuple2<String, Integer> input) throws Exception {

                return (input.f1 >= 10);
            }
        });

        s6.writeAsCsv("/Users/valerio/work/developing/gitlab/master-eit-flink/data/out.txt");

        try {
            env.execute();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

}