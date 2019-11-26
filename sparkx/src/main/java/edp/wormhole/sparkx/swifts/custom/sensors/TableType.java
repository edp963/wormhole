package edp.wormhole.sparkx.swifts.custom.sensors;

import java.io.Serializable;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author daemon
 * @Date 19/11/21 15:52
 * To change this template use File | Settings | File Templates.
 */
public enum TableType implements Serializable {
    EVENT(0),PROFILE(1),ITEM(2);

    private int index;
    private TableType(int index){
        this.index=index;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }
}
