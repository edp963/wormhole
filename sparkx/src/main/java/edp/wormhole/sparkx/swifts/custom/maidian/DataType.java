package edp.wormhole.sparkx.swifts.custom.maidian;

public enum DataType {
    TEXT(1), //文本
    NUMBER(2), //数值
    LOGIC(3), //逻辑
    TIME(4); //时间

    private Integer type;

    DataType(Integer type) {
        this.type = type;
    }

    public Integer getType() {
        return type;
    }

    public static DataType getDataType(Integer dataType){
        for(DataType t:DataType.values()){
            if(t.getType().equals(dataType)){
                return t;
            }
        }
        return null;
    }
}
