package master2017.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
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
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Iterator;

public class VehicleTelematics {

    private static DataStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> parsedStream;
    private static SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> parsedTimedStream;

    public static void main(String[] args) {

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
        DataStreamSource<String> stream = env.readTextFile(INPUT_FILE_PATH);

        // Map all the rows (String) to a tuple of 8 elements consisting of the converted fields (String -> Integer)
        parsedStream = stream
            .map(new MapFunction<String, Tuple8<Integer, Integer, Integer, Integer,
                                                Integer, Integer, Integer, Integer>>() {

                @Override
                public Tuple8<Integer, Integer, Integer, Integer,
                              Integer, Integer, Integer, Integer> map(String row) {

                    String fields[] = row.split(",");
                    return new Tuple8<>(
                            new Integer(fields[0]), new Integer(fields[1]), new Integer(fields[2]),
                            new Integer(fields[3]), new Integer(fields[4]), new Integer(fields[5]),
                            new Integer(fields[6]), new Integer(fields[7]));

                }

            });

        // Associate to the timestamp field an actual time in milliseconds that could be used for event windows
        parsedTimedStream = parsedStream
            .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple8<Integer, Integer, Integer, Integer,
                                                                                  Integer, Integer, Integer, Integer>>() {

                @Override
                public long extractAscendingTimestamp(Tuple8<Integer, Integer, Integer, Integer,
                                                             Integer, Integer, Integer, Integer> tuple) {
                    return (long) tuple.f0 * 1000;
                }

            });


        // Check for the alerts
        highSpeedAlert(OUTPUT_FOLDER_PATH + "/" + "speedfines.csv");
        avgSpeedAlert(OUTPUT_FOLDER_PATH + "/" + "avgspeedfines.csv");
        collisionAlert(OUTPUT_FOLDER_PATH + "/" + "accidents.csv");

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("=========================================================================================");

    }

    /* ALERTS IMPLEMENTATIONS *****************************************************************************************/

    //
    // 1st ALERT
    //

    private static void highSpeedAlert(String outputFilePath) {
        DataStream<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> highSpeedFines
                = parsedStream
                    .map(new FormatHighSpeedFields())   // re-arrange the fields
                    .filter(new HighSpeedFilter());     // only those with speed >= 90mph

        // Write the output into a new file
        highSpeedFines.writeAsCsv(outputFilePath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
    }

    /** Re-arrange the old tuples so to have the following structure:
     * f0: timestamp
     * f1: vehicle id
     * f2: xway
     * f3: segment
     * f4: direction
     * f5: speed
     */
    private static class FormatHighSpeedFields
            implements MapFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>,
            Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> {
        @Override
        public Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>
        map(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> oldTuple) {

            return new Tuple6<>(oldTuple.f0, oldTuple.f1, oldTuple.f3, oldTuple.f6, oldTuple.f5, oldTuple.f2);
        }
    }

    /** Keep only those tuples with speed >= 90mph */
    private static class HighSpeedFilter
            implements FilterFunction<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> {
        @Override
        public boolean filter(Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> tuple) {
            return tuple.f5 >= 90;
        }
    }

    //
    // 2nd ALERT
    //

    private static final int ENTRY_SEGMENT = 52;
    private static final int EXIT_SEGMENT = 56;

    private static void avgSpeedAlert(String outputFilePath) {

        SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> avgSpeedFines
            = parsedStream
                .filter(new SegmentFilter())        // get only those in between 52 and 56
                .map(new FormatAvgSpeedFields())    // re-arrange and add some fields
                .keyBy(2, 4)                 // key by id and direction
                .reduce(new UpdateFields())         // get the first and last timestamp, speeds sum and total count
                .filter(new ContainsAllSegments())  // only those who travelled all the segments

                /*
                 At this point we have filtered out all the cars that did not travel through all the segments.
                 However, for those who did travel, the reduce function created a new line with a new timestamp for
                 any event registered in the last segments (52 or 56, depending on the direction). For this reason we
                 have to report only the events with the highest exit time for every car and direction and discard the
                 others. To do this we key by id and direction, we create sessions that close as soon no new data is
                 available and only at that point we iterate over the window to get the very last event.
                 */

                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<Tuple8<Integer, Integer, Integer, Integer,
                                                               Integer, Integer, Integer, HashSet<Integer>>>() {
                    @Override
                    public long extractAscendingTimestamp(
                            Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, HashSet<Integer>> tuple) {
                        return (long) tuple.f1 * 1000;
                    }
                })                                                          // consider the exit timestamps
                .keyBy(2, 4)                                         // key by id and direction
                .window(EventTimeSessionWindows.withGap(Time.seconds(31)))  // get windows
                .apply(new GetLastWindowEvent())                            // get the very last event of the window
                .map(new ComputeAvgSpeed())                                 // get the avg speed
                .filter(new AvgSpeedFinesFilter());                         // get only those with avg speed >= 60mph

        avgSpeedFines.writeAsCsv(outputFilePath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
    }

    /** Remove all the events whose segment is not in between 52 and 56 */
    private static class SegmentFilter
            implements FilterFunction<Tuple8<Integer, Integer, Integer, Integer,
            Integer, Integer, Integer, Integer>> {

        @Override
        public boolean filter(Tuple8<Integer, Integer, Integer, Integer,
                Integer, Integer, Integer, Integer> event) {

            return (event.f6 >= ENTRY_SEGMENT && event.f6 <= EXIT_SEGMENT);
        }
    }

    /** Re-arrange the old tuples so to have the following structure:
     * f0: smallest timestamp (recorded by the vehicle in the segments 52-56)
     * f1: highest timestamp (recorded by the vehicle in the segments 52-56)
     * f2: vehicle id
     * f3: xway
     * f4: direction
     * f5: speed total (i.e. simple sum of all the speeds recorded in segments 52-56)
     * f6: a simple count
     * f7: segment set (i.e. a set of all the segments crossed by the vehicle)
     */
    private static class FormatAvgSpeedFields
            implements MapFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>,
                                   Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, HashSet<Integer>>> {

        @Override
        public Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, HashSet<Integer>>
                map(Tuple8<Integer, Integer, Integer, Integer,
                           Integer, Integer, Integer, Integer> oldTuple) {

            HashSet<Integer> segmentSet = new HashSet<>();
            segmentSet.add(oldTuple.f6);

            return new Tuple8<>(oldTuple.f0, oldTuple.f0, oldTuple.f1, oldTuple.f3,
                                oldTuple.f5, oldTuple.f2, 1, segmentSet);

        }
    }

    /** Reduce a stream keyed by id and direction so to:
     * - get the minimum timestamp recorded
     * - get the maximum timestamp recorded
     * - get the sum of all the speeds
     * - get the sum of all the counts
     * - get a set of all the segments
     */
    private static class UpdateFields
            implements ReduceFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, HashSet<Integer>>> {

        @Override
        public Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, HashSet<Integer>>
            reduce(Tuple8<Integer, Integer, Integer, Integer,
                          Integer, Integer, Integer, HashSet<Integer>> tuple1,
                   Tuple8<Integer, Integer, Integer, Integer,
                          Integer, Integer, Integer, HashSet<Integer>> tuple2) {

            int minTime = (tuple1.f0 <= tuple2.f0) ? tuple1.f0 : tuple2.f0;
            int maxTime = (tuple1.f1 >= tuple2.f1) ? tuple1.f1 : tuple2.f1;

            int speed = tuple1.f5 + tuple2.f5;
            int count = tuple1.f6 + tuple2.f6;

            HashSet<Integer> segmentSet = tuple1.f7;
            segmentSet.addAll(tuple2.f7);

            return new Tuple8<>(minTime, maxTime, tuple1.f2, tuple1.f3, tuple1.f4, speed, count, segmentSet);

        }

    }

    /** Discard all the tuples whose segment set does not contain all the relevant segments (i.e. 52, 53, 54, 55, 56) */
    private static class ContainsAllSegments
            implements FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, HashSet<Integer>>> {
        final Integer[] allSegments = {52, 53, 54, 55, 56};

        @Override
        public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, HashSet<Integer>> tuple) {

            for (Integer segment : allSegments) {
                if (!tuple.f7.contains(segment))
                    return false;
            }

            return true;
        }
    }

    private static class GetLastWindowEvent
            implements WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, HashSet<Integer>>,
            Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, HashSet<Integer>>,
            Tuple, TimeWindow> {

        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, HashSet<Integer>>> iterable, Collector<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, HashSet<Integer>>> collector) {
            Iterator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, HashSet<Integer>>> window = iterable.iterator();
            Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, HashSet<Integer>> lastWindowEvent = null;

            while (window.hasNext()) {
                lastWindowEvent = window.next();
            }

            collector.collect(lastWindowEvent);
        }
    }

    /** Given the tuples with the sum of all the speeds and the total count of events recorded, return the tuples with
     * the avg speed.
     */
    private static class ComputeAvgSpeed
            implements MapFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, HashSet<Integer>>,
            Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> {

        @Override
        public Tuple6<Integer, Integer, Integer, Integer, Integer, Double>
        map(Tuple8<Integer, Integer, Integer, Integer,
                Integer, Integer, Integer, HashSet<Integer>> oldTuple) {

            return new Tuple6<>(oldTuple.f0, oldTuple.f1, oldTuple.f2, oldTuple.f3, oldTuple.f4,
                    (double) (oldTuple.f5 / oldTuple.f6));
        }
    }

    /** Filter all the tuples whose avg speed is not above 60 mph */
    private static class AvgSpeedFinesFilter
            implements FilterFunction<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> {
        @Override
        public boolean filter(Tuple6<Integer, Integer, Integer, Integer, Integer, Double> tuple) {
            return tuple.f5 >= 60;
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