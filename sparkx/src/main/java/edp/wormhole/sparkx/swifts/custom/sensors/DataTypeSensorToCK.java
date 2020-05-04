package edp.wormhole.sparkx.swifts.custom.sensors;

import java.io.Serializable;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author daemon
 * @Date 19/11/18 16:44
 * To change this template use File | Settings | File Templates.
 */
public enum DataTypeSensorToCK implements Serializable {
    NUMBER(1,"Int64"),
    STRING(2,"String"),
    LIST(3,"String"),
    DATE(4,"Int64"),
    DATETIME(5,"Int64"),
    BOOL(6,"Int8"),
    UNKNOWN(-1,"");

    private int index;
    private String clickHouseDataType;

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getClickHouseDataType() {
        return clickHouseDataType;
    }

    public void setClickHouseDataType(String clickHouseDataType) {
        this.clickHouseDataType = clickHouseDataType;
    }

    DataTypeSensorToCK(int index, String clickHouseDataType){
        this.index=index;
        this.clickHouseDataType=clickHouseDataType;
    }

    public static DataTypeSensorToCK indexOf(int index){
        for(DataTypeSensorToCK type:values()){
            if(type.getIndex()==index){
                return type;
            }
        }
        return UNKNOWN;
    }
}
