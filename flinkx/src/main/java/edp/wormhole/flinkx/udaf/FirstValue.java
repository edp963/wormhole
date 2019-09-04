package edp.wormhole.flinkx.udaf;

import org.apache.flink.table.functions.AggregateFunction;

import java.util.LinkedList;

public class FirstValue extends AggregateFunction<Long, FirstAccum> {

    public FirstAccum createAccumulator() {
        return new FirstAccum(new LinkedList<Long>());
    }

    public Long getValue(FirstAccum accumulator) {
        return accumulator.windowQue.peek();
    }

    public void accumulate(FirstAccum accumulator, Long cValue) {
        if(null == accumulator.windowQue) {
            accumulator.windowQue = new LinkedList<Long>();
        }
        accumulator.windowQue.offer(cValue);
    }

    public void retract(FirstAccum accumulator, Long cValue) {
        if(null != accumulator.windowQue && accumulator.windowQue.size() > 0) {
            accumulator.windowQue.poll();
        }
    }
}
