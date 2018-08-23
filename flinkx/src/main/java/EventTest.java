import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class EventTest {

    public static void main(String[] s) {

        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        DataStream<Event> input = env.fromElements(
                new Event(1, "aa", "2018-05-14 10:29:15"),
                new Event(1, "ab", "2018-05-14 10:29:25"),
                new Event(3, "ac", "2018-05-14 10:29:35"),
                new Event(4, "ad", "2018-05-14 10:29:45"),
                new Event(5, "ae", "2018-05-14 10:29:55"));


        DataStream<Event> withTimestampsAndWatermarks =
                input.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Event>(Time.seconds(10)) {
                    @Override
                    public long extractTimestamp(Event element) {
                        try {
                            Date dt = formatter.parse(element.umsTs);
                            return dt.getTime();
                        } catch (ParseException e) {
                            e.printStackTrace();
                            return 0;
                        }
                    }
                });

        KeyedStream<Event, Long> partitionedInput = withTimestampsAndWatermarks.keyBy(new KeySelector<Event, Long>() {
            public Long getKey(Event e) {
                return e.id;
            }
        });

        Pattern<Event, Event> pattern = Pattern.<Event>begin("start")
                .subtype(Event.class)
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) {
                        return event.name.startsWith("a");
                    }
                }).within(Time.seconds(30));

        PatternStream<Event> patternStream = CEP.pattern(partitionedInput, pattern);

        DataStream<List<Event>> alerts = patternStream.select(
                new PatternSelectFunction<Event, List<Event>>() {
                    @Override
                    public List<Event> select(Map<String, List<Event>> pattern) {
                        List<Event> startEvent = pattern.get("start");
                        System.out.println("name:"+startEvent.get(0).name);
                        return startEvent;
                    }
                }
        );
        alerts.print();

        try {
            env.execute("start");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }


}


//class MyPatternSelectFunction<Event, List<Event>> extends PatternSelectFunction<Event, List<Event>> {
//    @Override
//    public List<Event> select(Map<String, List<Event>> pattern) {
//        List<Event> startEvent = pattern.get("start");
//
//        return startEvent;
//    }
//}

//        DataStream<Event> withTimestampsAndWatermarks =
//                input.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Event>() {
//
//                    @Override
//                    public long extractAscendingTimestamp(Event element) {
//                        try {
//                            Date dt = formatter.parse(element.getUmsTs());
//                            return dt.getTime();
//                        } catch (ParseException e) {
//                            e.printStackTrace();
//                            return 0;
//                        }
//                    }
//                });