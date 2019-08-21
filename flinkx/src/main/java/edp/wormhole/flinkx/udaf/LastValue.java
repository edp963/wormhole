package edp.wormhole.flinkx.udaf;

import org.apache.flink.table.functions.AggregateFunction;

public class LastValue extends AggregateFunction<Long, LastAccum> {

    public LastAccum createAccumulator() {
        return new LastAccum(null);
    }

    public Long getValue(LastAccum accumulator) {
        return accumulator.last;
    }

    public void accumulate(LastAccum accumulator, Long cValue) {
        accumulator.last = cValue;
    }

    public void retract(LastAccum accumulator, Long cValue) {}
}
