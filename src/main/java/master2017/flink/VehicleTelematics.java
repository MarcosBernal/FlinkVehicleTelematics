package master2017.flink;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

public class VehicleTelematics {

    private static DataStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> parsedStream;
    private static SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> parsedTimedStream;

    public static void main(String[] args) throws Exception {

        if(args.length != 2){
            System.out.println(">> Needs two arguments: \n\t - input_file_path \n\t - output_folder_path \n\n" +
                    "Example of usage: \nmvn exec:java -Dexec.mainClass=\"master2017.flink.VehicleTelematics\" input_file_path output_folder_path");
            System.exit(1);
        }

        final String INPUT_FILE_PATH = args[0];
        final String OUTPUT_FOLDER_PATH = args[1];

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Import the file
        DataStreamSource<String> stream = env.readTextFile(INPUT_FILE_PATH)
                .setParallelism(1); // Get rid of warning of Timestamp monotony violated -> sequential

        // Map all the rows (String) to a tuple of 8 elements consisting of the converted fields (String -> Integer)
        parsedStream = stream
            .map( tuple -> tuple.split(","))
            .setParallelism(1) // Get rid of warning of Timestamp monotony violated -> sequential
            .map( fields -> new Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>(
                    Integer.parseInt(fields[0]), Integer.parseInt(fields[1]), Integer.parseInt(fields[2]),
                    Integer.parseInt(fields[3]), Integer.parseInt(fields[4]), Integer.parseInt(fields[5]),
                    Integer.parseInt(fields[6]), Integer.parseInt(fields[7])))
            .setParallelism(1); // Get rid of warning of Timestamp monotony violated -> sequential



        // Associate to the timestamp field an actual time in milliseconds that could be used for event windows
        parsedTimedStream = parsedStream
            .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple8<Integer, Integer, Integer, Integer,
                                                                                  Integer, Integer, Integer, Integer>>() {

                @Override
                public long extractAscendingTimestamp(Tuple8<Integer, Integer, Integer, Integer,
                                                             Integer, Integer, Integer, Integer> tuple) {
                    return tuple.f0 * 1000;
                }

            }).setParallelism(1); // Get rid of warning of Timestamp monotony violated -> sequential


        /* ALERTS IMPLEMENTATIONS *****************************************************************************************/

        //
        // 1st ALERT - highSpeedAlert
        //

        
        parsedTimedStream
                // keep only those with speed >= 90mph
                .filter( tuple -> tuple.f2 >= 90)

                // re-arrange the fields
                .map( tuple -> new Tuple6<>(tuple.f0, tuple.f1, tuple.f3, tuple.f6, tuple.f5, tuple.f2))

                // write the output into a new file
                .writeAsCsv(OUTPUT_FOLDER_PATH + "/" + "speedfines.csv",
                            FileSystem.WriteMode.OVERWRITE)

                // set parallelism to 1 to create only ONE file
                .setParallelism(1);


        //
        // 2nd ALERT - avgSpeedAlert
        //
        DataStream<Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer[]>> beautiful_bug = parsedTimedStream
                .filter(event -> event.f6 >= 52 && event.f6 <= 56 &&                      // get only those in between 52 and 56
                            ( (event.f4 > 0 && event.f4 < 4)                                // and only vehicles that are in the travel lane
                                || (event.f6 == 52 && event.f4 < 4)                           // except if vehicle enters in seg 52
                                || (event.f6 == 56 && event.f4 > 0)))                        // or exits on seg 56
                .map(tuple -> new Tuple9<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer, Integer[]>(tuple.f0, tuple.f0, tuple.f2, tuple.f3, tuple.f1, tuple.f5, tuple.f6  // moving f1(VID) to f7
                        , 1, new Integer[]{52,53,54,55,56})     )             // Added an array in the 10th pos and a count variable in the 9th pos
                .keyBy(4, 3, 5)                                                    // key by id, xway and direction
                .window(EventTimeSessionWindows.withGap(Time.seconds(31)))                // get windows assuming Timestamp monotony
                .reduce((t1, t2) -> {
                    t1.f8[t1.f6-52] = 0;
                    t1.f8[t2.f6-52] = 0;
                    int minTime = (t1.f0 < t2.f0) ? t1.f0 : t2.f0;
                    int maxTime = (t1.f1 > t2.f1) ? t1.f1 : t2.f1;
                    return new Tuple9(minTime, maxTime, t1.f2 + t2.f2,
                            t1.f3, t1.f4, t1.f5, t1.f6, t1.f7 + t2.f7, t1.f8);
                });

        beautiful_bug
                .filter(t -> t.f2/t.f7 >= 60 && t.f8[0]+t.f8[1]+t.f8[2]+t.f8[3]+t.f8[4] == 0)
                .map(event -> new Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>(event.f0, event.f1, event.f4, event.f3, event.f5, event.f2/event.f7))
                .writeAsCsv(OUTPUT_FOLDER_PATH + "/" + "avgspeedfines.csv", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);                                                      // setPar to 1 to create only ONE file

        /**  0      1      2     3    4     5     6    7    8
         *  Time,  VID,   Spd, XWay, Lane, Dir,  Seg, Pos
		 *
		 *  Time1, Time2, Spd, XWay, VID,  Dir,  Seg, count, segList
		 *
         *  Time1, Time2, VID, XWay, Dir, AvgSpd
         */
        //
        //
        // 3rd ALERT - collisionAlert
        //

        parsedTimedStream
                .keyBy(1, 3, 5)                                                      // key by id, highway and direction
                .filter( tuple -> tuple.f2 == 0)                                            // only vehicles with null speed can crash
                .keyBy(1, 3, 5)                                                      // key by id, highway and direction
                .window(SlidingEventTimeWindows.of(Time.seconds(30 * 4), Time.seconds(30))) // get windows of 2min every 30secs

                .apply(new CheckForCollisions())

                // write the output into a new file
                .writeAsCsv(OUTPUT_FOLDER_PATH + "/" + "accidents.csv",
                            FileSystem.WriteMode.OVERWRITE)

                // set parallelism to 1 to create only ONE file
                .setParallelism(1);


        env.execute();

        System.out.println("=========================================================================================");
        System.out.println("=========================================================================================");


    }


    /** Given a window of 2mins of events keyed by id, highway and direction, it checks if any collision happened */
    private static class CheckForCollisions
            implements WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>,
                                      Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>,
                                      Tuple, TimeWindow> {

        Iterator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> window;
        Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> event;
        int initTime, initPos, validEventsCount;

        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow,
                          Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> input,
                          Collector<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> output) {



            // Get the current window (REMEMBER: it is keyed by ID, Highway and Direction!)
            window = input.iterator();

            // Get the first event of the window
            event = window.next();

            // Initialize time and position with the first event (REMEMBER: it might not be the first event in real time!)
            initTime = event.f0;
            initPos = event.f7;

            // ...and count one valid event
            validEventsCount = 1;

            // Go on checking the other events in the window
            while(window.hasNext()){

                event = window.next();

                // If the car moved then we do not have a crash
                if(event.f7 != initPos)
                    return;

                // Otherwise we count it as a valid event
                validEventsCount++;

                // and check if the event came out of order by updating the minTime
                initTime = (event.f0 < initTime) ? event.f0 : initTime;

            }

            // if 4 or more valid events are reported in the same window, then report an alert
            if(validEventsCount >= 4)
                output.collect(new Tuple7<>(initTime, event.f0, event.f1, event.f3, event.f6, event.f5, initPos));

        }
    }
}