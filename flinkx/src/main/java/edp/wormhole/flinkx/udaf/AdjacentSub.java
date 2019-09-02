package edp.wormhole.flinkx.udaf;

import org.apache.flink.table.functions.AggregateFunction;

public class AdjacentSub extends AggregateFunction<Long, AdjacentSubAccum> {

    public AdjacentSubAccum createAccumulator() {
        return new AdjacentSubAccum(null, null);
    }

    public Long getValue(AdjacentSubAccum accumulator) {
        if(null != accumulator.first && null != accumulator.last) {
            if(accumulator.last < accumulator.first)
                return accumulator.last;
            else
                return accumulator.last - accumulator.first;
        } else {
            return 0L;
        }
    }

    public void accumulate(AdjacentSubAccum accumulator, Long cValue) {
        accumulator.first = accumulator.last;
        accumulator.last = cValue;
    }

    public void retract(AdjacentSubAccum accumulator, Long cValue) {}
}
