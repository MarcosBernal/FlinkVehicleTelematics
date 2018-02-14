package master2017.flink;

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
import org.apache.flink.streaming.api.windowing.time.Time;
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
        DataStreamSource<String> stream = env.readTextFile(INPUT_FILE_PATH).setParallelism(1); // Get rid of warning of Timestamp monotony violated -> sequential

        // Map all the rows (String) to a tuple of 8 elements consisting of the converted fields (String -> Integer)
        parsedStream = stream
            .map( tuple -> tuple.split(",")).setParallelism(1) // Get rid of warning of Timestamp monotony violated -> sequential
            .map( fields -> new Tuple8<>( new Integer(fields[0]), new Integer(fields[1]), new Integer(fields[2]),
                    new Integer(fields[3]), new Integer(fields[4]), new Integer(fields[5]),
                    new Integer(fields[6]), new Integer(fields[7])))
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

/*
        // 1st ALERT highSpeedAlert
        //

        parsedTimedStream
                .keyBy(0)                                                                                // Using the timestamp as key to split the dataset
                .filter( tuple -> tuple.f2 >= 90)                                                               // only those with speedSum >= 90mph
                .map( tuple -> new Tuple6<>(tuple.f0, tuple.f1, tuple.f3, tuple.f6, tuple.f5, tuple.f2))        // re-arrange the fields
                .writeAsCsv(OUTPUT_FOLDER_PATH + "/" + "speedfines.csv", FileSystem.WriteMode.OVERWRITE)   // Write the output into a new file
                .setParallelism(1);                                                                             // setPar to 1 to create only ONE file
*/

        //
        // 2nd ALERT avgSpeedAlert
        //

        parsedTimedStream
                .filter(event -> event.f6 >= 52 && event.f6 <= 56)                  // get only those in between 52 and 56
                .keyBy(1, 3, 5)                                              // key by id, xway and direction
                .window(EventTimeSessionWindows.withGap(Time.seconds(31)))          // get event windows that close as soon as a car doesn't give information for more than 30 secs
                .apply(new ComputeWindowEvent())                                    // get the tuple if vehicle went by all segments and avg speedSum >= 60mph
                .writeAsCsv(OUTPUT_FOLDER_PATH + "/" + "avgspeedfines.csv",
                            FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);                                                 // setPar to 1 to create only ONE file

/*
        //
        // 3rd ALERT
        //

        parsedTimedStream
                .filter( tuple -> tuple.f2 == 0)                                // only vehicles with null speedSum can crash
                .keyBy(1, 3, 5)                                                 // key by id, highway and direction
                .window(SlidingEventTimeWindows.of(Time.seconds(30 * 4),
                                                   Time.seconds(30)))           // get windows of 2min every 30secs
                .apply(new CheckForCollisions())
                .writeAsCsv(OUTPUT_FOLDER_PATH + "/" + "accidents.csv",
                            FileSystem.WriteMode.OVERWRITE)
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
    private static class ComputeWindowEvent
            implements WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>,
            Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>, Tuple, TimeWindow> {

        Iterator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> window;
        Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> event;
        int eventCount, speedSum, minTime, maxTime;
        boolean oneway;
        Set<Integer> segmentSet;

        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow,
                Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterable,
                    Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> collector) {

            eventCount = 0;
            speedSum = 0;
            minTime = Integer.MAX_VALUE;
            maxTime = 0;
            oneway = true;
            segmentSet = new HashSet<>();

            window = iterable.iterator();

            while (window.hasNext() /*&& oneway*/) {

                event = window.next();
                eventCount ++;

                minTime = (event.f0 <= minTime) ? event.f0 : minTime;
                maxTime = (event.f0 >= maxTime) ? event.f0 : maxTime;

                speedSum += event.f2;
                segmentSet.add(event.f6);
                //oneway = segmentSet.get(0) == 52 && segmentSet.get(segmentSet.size() - 1) <= event.f6
                //        || segmentSet.get(0) == 56 && segmentSet.get(segmentSet.size() - 1) >= event.f6;
                // One way means from 52 to 56 without return

            }

            if(!segmentSet.containsAll(ALL_SEGMENTS))
                return;

            if(speedSum / eventCount < 60)
                return;

            collector.collect(new Tuple6<>(minTime, maxTime, event.f1, event.f3, event.f5, speedSum / eventCount));
        }
    }


    /** Given a window of 2mins of events keyed by id, checks if any collision happened in those 2 minutes */
    private static class CheckForCollisions
            implements WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>,
                                      Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>,
                                      Tuple, TimeWindow> {

        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow,
                          Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> input,
                          Collector<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> output) {

            int initTime, initPos, numberOfEvents;

            // Get the current window (REMEMBER: it is keyed by ID, Highway and Direction!)
            Iterator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> window
                    = input.iterator();

            // Get the first event of the window
            Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> event
                    = window.next();

            // If the event is null or the speedSum is not 0
            //  then we cannot raise an alert for collision in the current window
            //if (event == null)
            //    return;

            // If the event is valid, initialize some variables
            initTime = event.f0;
            initPos = event.f7;

            // ...and count one event
            numberOfEvents = 1;

            // Go on checking the other events in the window
            while(window.hasNext()){

                event = window.next();

                // If the car moved we cannot report an alert
                if(event.f7 != initPos)
                    return;

                // Otherwise we count it as a valid event
                numberOfEvents++;
            }

            // if 4 or more events are reported in the same window, then report an alert
            if(numberOfEvents >= 4)
                output.collect(new Tuple7<>(initTime, event.f0, event.f1, event.f3, event.f6, event.f5, initPos));

        }
    }
}