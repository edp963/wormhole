package edp.wormhole.sparkx.swifts.custom.sensors;


import org.apache.spark.sql.types.DataType;

import java.io.Serializable;

import static org.apache.spark.sql.types.DataTypes.*;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author daemon
 * @Date 19/11/18 16:44
 * To change this template use File | Settings | File Templates.
 */
public enum DataTypeSparkToCK implements Serializable {

    LONG(LongType,"Int64"),
    STRING(StringType,"String"),
    INTEGER(IntegerType,"Int8"),
    BOOLEAN(BooleanType,"Int8"),
    FLOAT(FloatType,"String"),
    DOUBLE(DoubleType,"String"),
    DATE(DateType,"String"),
    BINARY(BinaryType,"String");


    /*case 1 =>LongType
    case 2 =>StringType
    case 3 =>StringType
    case 4 =>LongType
    case 5 =>LongType
    case 6 =>IntegerType
    case _ =>StringType*/


    private DataType sparkSqlType;
    private String clickHouseDataType;

    public DataType getSparkSqlType() {
        return sparkSqlType;
    }

    public void setSparkSqlType(DataType sparkSqlType) {
        this.sparkSqlType = sparkSqlType;
    }

    public String getClickHouseDataType() {
        return clickHouseDataType;
    }

    public void setClickHouseDataType(String clickHouseDataType) {
        this.clickHouseDataType = clickHouseDataType;
    }

    DataTypeSparkToCK(DataType sparkSqlType, String clickHouseDataType){
        this.sparkSqlType=sparkSqlType;
        this.clickHouseDataType=clickHouseDataType;
    }

    public static DataTypeSparkToCK sparkTypeOf(DataType sparkSqlType){
        for(DataTypeSparkToCK type:values()){
            if(type.getSparkSqlType()==sparkSqlType){
                return type;
            }
        }
        return null;
    }
}
