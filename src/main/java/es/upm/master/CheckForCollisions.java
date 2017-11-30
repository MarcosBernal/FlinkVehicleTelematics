package es.upm.master;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class CheckForCollisions
        implements WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>,
                    Tuple8<String, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple, TimeWindow> {

    @Override
    public void apply(Tuple tuple, TimeWindow timeWindow,
                      Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> input,
                      Collector<Tuple8<String, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> ouput)
            throws Exception {

        int initTime, lastTime, initPos, id, xway, lane, dir, seg;

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
        lane = firstEvent.f4;
        dir = firstEvent. f5;
        seg = firstEvent.f6;
        initPos = firstEvent.f7;

        // Go on checking the other events in the window
        while(window.hasNext()){

            Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> event = window.next();

            // If the event is null or the current position is not the same as the original position
            //  then we cannot raise an alert in the current window
            if (event == null || event.f7 != initPos)
                return;

            lastTime = event.f0;
        }

        // If all the events in the window have null speed and same position then output the alert
        ouput.collect(new Tuple8<>("[" + initTime + " > " + lastTime + "]", id, 0, xway, lane, dir, seg, initPos));

    }
}
