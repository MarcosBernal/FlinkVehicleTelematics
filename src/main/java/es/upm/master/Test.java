package es.upm.master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test {

    private static final int TIM_FIELD = 0;
    private static final int VID_FIELD = 1;
    private static final int SPD_FIELD = 2;
    private static final int WAY_FIELD = 3;
    private static final int LAN_FIELD = 4;
    private static final int DIR_FIELD = 5;
    private static final int SEG_FIELD = 6;
    private static final int POS_FIELD = 7;

    // TODO: !!! CHANGE HERE YOUR DEFAULT INPUT AND OUTPUT FOLDERS  !!!!!
    private static final String INPUT_FOLDER_PATH = "/media/sf_Shared2Ubuntu/flink/project/data/in/";
    private static final String OUTPUT_FOLDER_PATH = "/media/sf_Shared2Ubuntu/flink/project/data/out/";

    private static DataStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> parsedStream;

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Import the file TODO: change input filepath
        DataStreamSource<String> stream = env.readTextFile(INPUT_FOLDER_PATH + "traffic-3xways.txt");

        // Map all the lines (String) to a tuple of 8 elements consisting of the converted fields (String -> Integer)
        parsedStream = stream
                .map(new MapFunction<String, Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> map(String s) throws Exception {
                        String fields[] = s.split(",");
                            return new Tuple8<>(
                                    new Integer(fields[0]), new Integer(fields[1]), new Integer(fields[2]),
                                    new Integer(fields[3]), new Integer(fields[4]), new Integer(fields[5]),
                                    new Integer(fields[6]), new Integer(fields[7]));
                    }});

        highSpeedAlert("highSpeedAlert.csv");

        avgSpeedAlert("avgSpeedAlert.csv");

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
        DataStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> highSpeedFines = parsedStream
                .filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> tuple) throws Exception {
                        return tuple.f2 >= 90;
                    }});

        // Write the output into a new file TODO: change output filepath
        highSpeedFines.writeAsCsv(OUTPUT_FOLDER_PATH + outputFileName);
    }

    private static void avgSpeedAlert(String outputFileName) {
        // TODO: implementation
    }

    private static void collisionAlert(String outputFileName) {
        // TODO: implementation
    }

}



class Row2<T1, T2> extends Tuple2<Integer, Integer> {
    public Row2(Integer value0, Integer value1) {
        super(value0, value1);
    }
}

class Row3<T1, T2, T3> extends Tuple3<Integer, Integer, Integer> {
    public Row3(Integer value0, Integer value1, Integer value2) {
        super(value0, value1, value2);
    }
}

class Row4<T1, T2, T3, T4> extends Tuple4<Integer, Integer, Integer, Integer> {
    public Row4(Integer value0, Integer value1, Integer value2, Integer value3) {
        super(value0, value1, value2, value3);
    }
}

class Row5<T1, T2, T3, T4, T5> extends Tuple5<Integer, Integer, Integer, Integer, Integer> {
    public Row5(Integer value0, Integer value1, Integer value2, Integer value3, Integer value4) {
        super(value0, value1, value2, value3, value4);
    }
}

class Row6<T1, T2, T3, T4, T5, T6> extends Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> {
    public Row6(Integer value0, Integer value1, Integer value2, Integer value3, Integer value4, Integer value5) {
        super(value0, value1, value2, value3, value4, value5);
    }
}

class Row7<T1, T2, T3, T4, T5, T6, T7>  extends Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> {
    public Row7(Integer value0, Integer value1, Integer value2, Integer value3, Integer value4, Integer value5, Integer value6) {
        super(value0, value1, value2, value3, value4, value5, value6);
    }
}

class Row8<T1, T2, T3, T4, T5, T6, T7, T8>
        extends Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> {

    public Row8(Integer value0, Integer value1, Integer value2, Integer value3, Integer value4, Integer value5, Integer value6, Integer value7) {
        super(value0, value1, value2, value3, value4, value5, value6, value7);
    }
}