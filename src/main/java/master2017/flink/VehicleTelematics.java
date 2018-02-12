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


        // Check for the alerts
        //collisionAlert(OUTPUT_FOLDER_PATH + "/" + "accidents.csv");




        /* ALERTS IMPLEMENTATIONS *****************************************************************************************/

        //
        // 1st ALERT highSpeedAlert
        //

        parsedTimedStream
                .keyBy(0)                                                                                // Using the timestamp as key to split the dataset
                .filter( tuple -> tuple.f2 >= 90)                                                               // only those with speed >= 90mph
                .map( tuple -> new Tuple6<>(tuple.f0, tuple.f1, tuple.f3, tuple.f6, tuple.f5, tuple.f2))        // re-arrange the fields
                .writeAsCsv(OUTPUT_FOLDER_PATH + "/" + "speedfines.csv", FileSystem.WriteMode.OVERWRITE)   // Write the output into a new file
                .setParallelism(1);                                                                             // setPar to 1 to create only ONE file


        //
        // 2nd ALERT avgSpeedAlert
        //

        parsedTimedStream
                .filter(event -> event.f6 >= 52 && event.f6 <= 56)                        // get only those in between 52 and 56
                .keyBy(1, 3, 5)                                                    // key by id, xway and direction
                .window(EventTimeSessionWindows.withGap(Time.seconds(31)))                // get windows
                .apply(new ComputeWindowEvent())                                          // get the tuple if vehicle went by all segments and avg speed >= 60mph
                .writeAsCsv(OUTPUT_FOLDER_PATH + "/" + "avgspeedfines.csv", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);                                                       // setPar to 1 to create only ONE file

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

        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow,
                Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterable,
                    Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> collector) throws Exception {


            Iterator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> window = iterable.iterator();
            Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> event = null;

            int count = 0;
            int speed = 0;
            int minTime = Integer.MAX_VALUE;
            int maxTime = 0;
            boolean oneway = true;
            ArrayList<Integer> segmentSet = new ArrayList<Integer>();

            while (window.hasNext() && oneway) {
                event = window.next();

                minTime = (event.f0 <= minTime) ? event.f0 : minTime;
                maxTime = (event.f0 >= maxTime) ? event.f0 : maxTime;

                speed += event.f2;
                count += 1;
                segmentSet.add(event.f6);
                oneway = (segmentSet.get(0) == 52 && segmentSet.get(segmentSet.size()-1) <= event.f6
                        || segmentSet.get(0) == 56 && segmentSet.get(segmentSet.size()-1) >= event.f6) ? true : false; // One way means from 52 to 56 without return

            }

            if(oneway && segmentSet.contains(new Integer(52)) && segmentSet.contains(new Integer(53)) && segmentSet.contains(new Integer(54)) && segmentSet.contains(new Integer(55)) && segmentSet.contains(new Integer(56)))
               if(speed/count >= 60) //Speed
                   collector.collect(new Tuple6<>(minTime, maxTime, event.f1, event.f3, event.f5, speed/count));
        }
    }



    //
    // 3rd ALERT
    //

    private static void collisionAlert(String outputFilePath) {

        // Once the stream is parsed and has time associated to it,
        // 1) group the events in the stream by ID
        // 2) check for collisions in 2mins-sized windows every 30secs
        SingleOutputStreamOperator<Tuple7<Integer, Integer, Integer,
                                          Integer, Integer, Integer, Integer>> collisions
            = parsedTimedStream
                .keyBy(1)                                                                // key by id
                .window(SlidingEventTimeWindows.of(Time.seconds(30 * 4), Time.seconds(30)))     // get windows of 2min every 30secs
                .apply(new CheckForCollisions());                                               // check for collisions

        // Write the output into a new file
        collisions.writeAsCsv(outputFilePath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

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

            int initTime, lastTime, initPos, id, xway, dir, seg;

            // Get the current window (REMEMBER: it is keyed by ID!)
            Iterator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> window
                    = input.iterator();

            // Get the first event of the window
            Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> firstEvent
                    = window.next();

            // If the event is null or the speed is not 0
            //  then we cannot raise an alert for collision in the current window
            if (firstEvent == null || firstEvent.f2 != 0)
                return;

            // If the event is valid, initialize some variables
            initTime = firstEvent.f0;
            lastTime = initTime;

            id = firstEvent.f1;
            xway = firstEvent.f3;
            dir = firstEvent. f5;
            seg = firstEvent.f6;
            initPos = firstEvent.f7;

            int event_number = 1;

            // Go on checking the other events in the window
            while(window.hasNext()){

                Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> event = window.next();

                // If the event is null or the current position is not the same as the original position
                //  then we cannot raise an alert in the current window
                if (event == null || event.f7 != initPos)
                    return;

                // Events might be not send in order when parallel exec (sorting the time)
                lastTime = (event.f0 > lastTime) ? event.f0 : lastTime;
                initTime = (initTime > event.f0) ? event.f0 : initTime;
                event_number++;
            }

            // If all the events in the window have null speed and same position then output the alert
            if(event_number >= 4)
                output.collect(new Tuple7<>(initTime, lastTime, id, xway, seg, dir, initPos));

        }
    }
}