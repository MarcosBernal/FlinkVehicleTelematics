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
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

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
        DataStreamSource<String> stream = env.readTextFile(INPUT_FILE_PATH).setParallelism(1); // Get rid of warning of Timestamp monotony violated -> sequential

        // Map all the rows (String) to a tuple of 8 elements consisting of the converted fields (String -> Integer)
        parsedStream = stream
            .map( tuple -> tuple.split(",")).setParallelism(1) // Get rid of warning of Timestamp monotony violated -> sequential
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


        // 1st ALERT highSpeedAlert
        //

        parsedTimedStream
                .filter( tuple -> tuple.f2 >= 90)                                                               // only those with speed >= 90mph
                .map( tuple -> new Tuple6<>(tuple.f0, tuple.f1, tuple.f3, tuple.f6, tuple.f5, tuple.f2))        // re-arrange the fields
                .writeAsCsv(OUTPUT_FOLDER_PATH + "/" + "speedfines.csv", FileSystem.WriteMode.OVERWRITE)   // Write the output into a new file
                .setParallelism(1);                                                                             // setPar to 1 to create only ONE file


        //
        // 2nd ALERT avgSpeedAlert
        //

        parsedTimedStream
                .filter(event -> event.f6 >= 52 && event.f6 <= 56 &&                      // get only those in between 52 and 56
                            ( (event.f4 > 0 && event.f4 < 4)                                // and only vehicles that are in the travel lane
                                || (event.f6 == 52 && event.f4 < 4)                           // except if vehicle enters in seg 52
                                || (event.f6 == 56 && event.f4 > 0)))                         // or exits on seg 56
                .keyBy(1, 3, 5)                                                    // key by id, xway and direction
                .window(EventTimeSessionWindows.withGap(Time.seconds(31)))                // get windows assuming Timestamp monotony
                .apply(new ComputeWindowEvent())                                          // get the tuple if vehicle went by all segments and avg speed >= 60mph
                .writeAsCsv(OUTPUT_FOLDER_PATH + "/" + "avgspeedfines.csv", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);                                                       // setPar to 1 to create only ONE file

        
        //
        //
        // 3rd ALERT
        //

        parsedTimedStream
                .filter( tuple -> tuple.f2 == 0)                                            // only vehicles with null speed can crash
                .keyBy(1, 3, 5)                                                      // key by id, highway and direction
                .window(SlidingEventTimeWindows.of(Time.seconds(30 * 4), Time.seconds(30))) // get windows of 2min every 30secs
                .apply(new CheckForCollisions())
                .writeAsCsv(OUTPUT_FOLDER_PATH + "/" + "accidents.csv", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        env.execute();

        System.out.println("=========================================================================================");
        System.out.println("=========================================================================================");


    }

    /**  0      1      2     3    4     5    6   7
     *  Time,  VID,   Spd, XWay, Lane, Dir, Seg, Pos
     *  Time1, Time2, VID, XWay, Dir,  AvgSpd
     */

    private static int EAST = 0;
    private static int WEST = 1;


    private static class ComputeWindowEvent // Assumed Timestamp monotony
            implements WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>,
            Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow,
                Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterable,
                    Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> collector) {


            Iterator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> window = iterable.iterator();
            Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> event = null;

            int count = 0;
            int speed = 0;
            int minTime = Integer.MAX_VALUE;
            int maxTime = 0;
            ArrayList<Integer> segments = new ArrayList<>();

            while(window.hasNext()){
                event = window.next();
                count += 1;
                speed += event.f2;
                if(!segments.contains(event.f6)) segments.add(event.f6);
                minTime = (event.f5 == EAST && event.f6 == 52 || event.f5 == WEST && event.f6 == 56) ? event.f0 : minTime;
                maxTime = (event.f5 == EAST && event.f6 == 56 || event.f5 == WEST && event.f6 == 52) ? event.f0 : maxTime;
            }

            if(speed/count >= 60 && minTime < maxTime && segments.size() == 5) //Speed
                collector.collect(new Tuple6<>(minTime, maxTime, event.f1, event.f3, event.f5, speed/count));

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

            // If the event is null or the speed is not 0
            //  then we cannot raise an alert for collision in the current window
            if (event == null)
                return;

            // If the event is valid, initialize some variables
            initTime = event.f0;
            initPos = event.f7;

            // ...and count one event
            numberOfEvents = 1;

            // Go on checking the other events in the window
            while(window.hasNext()){

                event = window.next();

                // If the event is null or the car moved we cannot report an alert
                if(event == null || event.f7 != initPos)
                    return;

                // Otherwise we count it
                numberOfEvents++;
            }

            // if 4 or more events are reported in the same window, then report an alert
            if(numberOfEvents >= 4)
                output.collect(new Tuple7<>(initTime, event.f0, event.f1, event.f3, event.f6, event.f5, initPos));

        }
    }
}