package edp.wormhole.flinkx.udaf;

public class AdjacentSubAccum {
    public Long first;
    public Long last;
    public AdjacentSubAccum(Long first, Long last) {
        this.first = first;
        this.last = last;
    }
}
