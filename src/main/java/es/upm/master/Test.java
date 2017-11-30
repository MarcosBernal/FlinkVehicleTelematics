package es.upm.master;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Test {

    private static String OUTPUT_FOLDER_PATH;
    private static DataStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> parsedStream;
    private static SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> parsedTimedStream;

    public static void main(String[] args) {

        // TODO: !!! CHANGE HERE YOUR DEFAULT INPUT AND OUTPUT FOLDERS  !!!!!
        String INPUT_FOLDER_PATH = args[0];
        OUTPUT_FOLDER_PATH = args[1];

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Import the file TODO: change input filepath
        DataStreamSource<String> stream = env.readTextFile(INPUT_FOLDER_PATH + "test-traffic-3xways.txt");

        // Map all the lines (String) to a tuple of 8 elements consisting of the converted fields (String -> Integer)
        parsedStream = stream
                .map(new MapFunction<String, Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> map(String s)
                            throws Exception {
                        String fields[] = s.split(",");
                            return new Tuple8<>(
                                    new Integer(fields[0]), new Integer(fields[1]), new Integer(fields[2]),
                                    new Integer(fields[3]), new Integer(fields[4]), new Integer(fields[5]),
                                    new Integer(fields[6]), new Integer(fields[7]));
                    }});

        // Associate to the timestamp field an actual time in milliseconds that could be used for event windows
        parsedTimedStream = parsedStream.assignTimestampsAndWatermarks(
                    new AscendingTimestampExtractor<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public long extractAscendingTimestamp(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> tuple) {
                return (long) tuple.f0 * 1000;
            }
        });


        // Check for the alerts
        highSpeedAlert("highSpeedAlert.csv");
        // avgSpeedAlert("avgSpeedAlert.csv");
        collisionAlert("collisionAlert.csv");

        try {
            env.execute();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        System.out.println("==========================================================");

    }

    private static void highSpeedAlert(String outputFileName) {
        // Once the stream is parsed filter those tuples whose speed (2nf field!) is larger or equal than 90
        DataStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>
                highSpeedFines = parsedStream
                    .filter(new HighSpeedFilter());

        // Write the output into a new file
        highSpeedFines.writeAsCsv(OUTPUT_FOLDER_PATH + outputFileName);
    }

    private static void avgSpeedAlert(String outputFileName) {
        // TODO: implementation
    }

    private static void collisionAlert(String outputFileName) {
        // Once the stream is parsed and has time associated to it,
        // 1) group the events in the stream by ID, then
        // 2) check for collisions in 2mins-sized windows every 30secs
        SingleOutputStreamOperator<Tuple8<String, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>
            collisions = parsedTimedStream
                .keyBy(1)
                .window(SlidingEventTimeWindows.of(Time.seconds(30 * 4), Time.seconds(30)))
                .apply(new CheckForCollisions());

        // Write the output into a new file
        collisions.writeAsCsv(OUTPUT_FOLDER_PATH + outputFileName);
    }

}