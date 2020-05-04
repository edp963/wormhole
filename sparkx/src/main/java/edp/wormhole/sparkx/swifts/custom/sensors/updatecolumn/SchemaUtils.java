package edp.wormhole.sparkx.swifts.custom.sensors.updatecolumn;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import edp.wormhole.externalclient.zookeeper.WormholeZkClient;
import edp.wormhole.sparkx.swifts.custom.sensors.DataTypeSensorToCK;
import edp.wormhole.sparkx.swifts.custom.sensors.DataTypeSparkToCK;
import edp.wormhole.sparkx.swifts.custom.sensors.entry.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author daemon
 * @Date 19/11/14 17:19
 * To change this template use File | Settings | File Templates.
 */
public class SchemaUtils implements Serializable {

    private static final Logger logger=LoggerFactory.getLogger(SchemaUtils.class);

    private ParamUtils paramUtils;

    private SensorsCkMetaClient metaClient;


    public SchemaUtils(ParamUtils paramUtils)throws Exception{
        this.paramUtils=paramUtils;
        this.metaClient=new SensorsCkMetaClient(paramUtils);
    }

    public void destroy(){
        if(metaClient!=null){
            metaClient.destroy();
        }
    }

    public boolean checkSchemaNeedChange(Map<String, String> columnMapDf) throws Exception{
        Map<String, String>  needAddColumns = new HashMap<>();
        Map<String,String> columnMapCk=metaClient.queryClickHouseSchema();
        for(String columnDfKey: columnMapDf.keySet()) {
            if(!columnMapCk.containsKey(columnDfKey)) {
                needAddColumns.put(columnDfKey, columnMapDf.get(columnDfKey));
            }
        }
        if(!needAddColumns.isEmpty()){
            metaClient.changeClickHouseSchema(needAddColumns);
            logger.info("schema change!!!!!!! needAddColumns is {}", needAddColumns);
        }
        return true;
    }

    public boolean checkClickHouseInitTable() throws Exception {
        return metaClient.checkAndCreateClickHouseTable();
    }

    public boolean initProject() throws Exception{
        this.checkClickHouseInitTable();
        return true;
    }

}
