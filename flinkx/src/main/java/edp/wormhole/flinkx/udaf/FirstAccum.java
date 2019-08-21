package edp.wormhole.flinkx.udaf;

import java.util.Queue;

public class FirstAccum {
    public Queue<Long> windowQue;

    public FirstAccum(Queue<Long> windowQue) {
        this.windowQue = windowQue;
    }
}
