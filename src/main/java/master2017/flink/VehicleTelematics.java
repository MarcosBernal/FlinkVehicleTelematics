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

    private static Set<Integer> ALL_SEGMENTS = new HashSet<Integer>(Arrays.asList(52, 53, 54, 55, 56));

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
            .map( fields -> new Tuple8<>( Integer.parseInt(fields[0]), Integer.parseInt(fields[1]), Integer.parseInt(fields[2]),
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

        /*
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
        */

        //
        // 2nd ALERT - avgSpeedAlert
        //

        parsedTimedStream
                // get only those in between 52 and 56
                .filter(event -> event.f6 >= 52 && event.f6 <= 56)
                /* POSSIBLE IMPROVEMENT:
                        ( (event.f4 > 0 && event.f4 < 4)        // and only vehicles that are in the travel lane
                        || (event.f6 == 52 && event.f4 < 4)     // except if vehicle enters in seg 52
                        || (event.f6 == 56 && event.f4 > 0))    // or exits at segment 54
                */

                // drop lane and position fields, add a new time field and a counter (=1)
                .map (tuple -> {
                    HashSet<Integer> segmentSet = new HashSet<>();
                    segmentSet.add(tuple.f6);
                    return new Tuple8<>(tuple.f0, tuple.f0, tuple.f1, tuple.f2, tuple.f3, tuple.f5, segmentSet, 1);
                })

                // key by id, xway and direction & reduce summing the speeds and the counters, expanding the set of segments and updating times
                .keyBy(2, 4, 5).reduce((tuple1, tuple2) -> {
                    tuple1.f0 = (tuple1.f0 <= tuple2.f0) ? tuple1.f0 : tuple2.f0;
                    tuple1.f1 = (tuple1.f1 >= tuple2.f1) ? tuple1.f1 : tuple2.f1;
                    tuple1.f3 += tuple2.f3;         // speedSum
                    tuple1.f6.addAll(tuple2.f6);    // segmentSet
                    tuple1.f7 += tuple2.f7;         // eventCount
                    return tuple1;
                })

                // filter cars that did not travel all the segments
                .filter(tuple -> tuple.f6.containsAll(ALL_SEGMENTS))

                // compute avg speed for remaining cars
                .map(tuple -> new Tuple6<>(tuple.f0, tuple.f1, tuple.f2, tuple.f4, tuple.f5, tuple.f3 / tuple.f7))

                // filter out cars with avg speed < 60
                .filter(tuple -> tuple.f5 >= 60)

                // keep the latest maxTime tuple (DOES NOT WORK AS EXPECT
                .keyBy(2,3,4).reduce((tuple1, tuple2) -> (tuple1.f1 > tuple2.f1) ? tuple1 : tuple2)

                /*
                // get event windows that close as soon as a car doesn't give information for more than 30 secs
                .window(EventTimeSessionWindows.withGap(Time.seconds(31)))

                // get the tuple if vehicle went by all segments and avg speedSum >= 60mph
                .apply(new ComputeWindowEvent())*/

                // write the output into a new file
                .writeAsCsv(OUTPUT_FOLDER_PATH + "/" + "avgspeedfines.csv",
                            FileSystem.WriteMode.OVERWRITE)

                // set parallelism to 1 to create only ONE file
                .setParallelism(1);

        /*
        //
        // 3rd ALERT - collisionAlert
        //

        parsedTimedStream
                // only vehicles with null speed can crash, so keep only those events
                .filter( tuple -> tuple.f2 == 0)

                // key by id, highway and direction
                .keyBy(1, 3, 5)

                // get sliding windows of 2min every 30secs
                .window(SlidingEventTimeWindows.of(Time.seconds(30 * 4),
                                                   Time.seconds(30)))

                // look into the window to identify crashes
                .apply(new CheckForCollisions())

                // write the output into a new file
                .writeAsCsv(OUTPUT_FOLDER_PATH + "/" + "accidents.csv",
                            FileSystem.WriteMode.OVERWRITE)

                // set parallelism to 1 to create only ONE file
                .setParallelism(1);
        */

        env.execute();

        System.out.println("=========================================================================================");
        System.out.println("=========================================================================================");


    }


    /**  0      1      2     3    4     5    6   7
     *  Time,  VID,   Spd, XWay, Lane, Dir, Seg, Pos
     *  Time1, Time2, VID, XWay, Dir,  AvgSpd
     */

    private static class ComputeWindowEvent // Assumed Timestamp monotony
            implements WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>,
            Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>, Tuple, TimeWindow> {

        Iterator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> window;
        Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> event;

        Set<Integer> segmentSet;
        int eventCount, speedSum, minTime, maxTime;


        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow,
                Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterable,
                    Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> collector) {

            eventCount = 0;
            speedSum = 0;
            minTime = Integer.MAX_VALUE;
            maxTime = 0;
            segmentSet = new HashSet<>();

            window = iterable.iterator();

            while (window.hasNext()) {

                // count an event more
                event = window.next();
                eventCount ++;

                // update the sum of the speeds of the vehicle
                speedSum += event.f2;

                // update the earliest and the latest time the car drove through the segments 52-56
                minTime = (event.f0 < minTime) ? event.f0 : minTime;
                maxTime = (event.f0 > maxTime) ? event.f0 : maxTime;

                // update the sets of segments the car drove through
                segmentSet.add(event.f6);

            }

            if(!segmentSet.containsAll(ALL_SEGMENTS) || minTime >= maxTime)
                return;

            if(speedSum / eventCount < 60)
                return;

            collector.collect(new Tuple6<>(minTime, maxTime, event.f1, event.f3, event.f5, speedSum / eventCount));

        }
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