package edp.wormhole.sparkx.swifts.custom.sensors;

import java.io.Serializable;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author daemon
 * @Date 19/11/19 15:49
 * To change this template use File | Settings | File Templates.
 */
public enum DataVersion implements Serializable {
    UNKNOWN(-1), VERSION1(1), VERSION2(2);

    private int index;

    private DataVersion(int index){
        this.index=index;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public static DataVersion indexOf(int index){
        for(DataVersion v:values()){
            if(v.getIndex()==index){
                return v;
            }
        }
        return UNKNOWN;
    }
}
