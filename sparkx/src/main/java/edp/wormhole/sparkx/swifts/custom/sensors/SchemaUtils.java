package edp.wormhole.sparkx.swifts.custom.sensors;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import edp.wormhole.externalclient.zookeeper.WormholeZkClient;
import edp.wormhole.sparkx.swifts.custom.sensors.entry.EventEntry;
import edp.wormhole.sparkx.swifts.custom.sensors.entry.PropertyColumnEntry;
import edp.wormhole.sparkx.swifts.custom.sensors.entry.PropertyEntry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
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

    private SensorsMetaClient metaClient;

    private Map<String,EventEntry> eventMap=new HashMap();

    private Map<String,PropertyEntry> proMap=new HashMap();

    private Map<Integer,PropertyColumnEntry> columnMap=new HashMap();

    private Map<String,PropertyColumnEntry> proColumnMap=new HashMap();

    private List<String> propertiesSortedList=new ArrayList();

    private String zkAddress;


    public SchemaUtils(ParamUtils paramUtils)throws Exception{
        this.paramUtils=paramUtils;
        this.metaClient=new SensorsMetaClient(paramUtils);
    }

    public void destroy(){
        if(metaClient!=null){
            metaClient.destroy();
        }
    }
    public Map<String, EventEntry> getEventMap() {
        return eventMap;
    }

    public Map<String, PropertyEntry> getProMap() {
        return proMap;
    }

    public Map<String, PropertyColumnEntry> getProColumnMap() {
        return proColumnMap;
    }

    public void setProColumnMap(Map<String, PropertyColumnEntry> proColumnMap) {
        this.proColumnMap = proColumnMap;
    }

    public List<String> getPropertiesSortedList() {
        return propertiesSortedList;
    }

    public void setPropertiesSortedList(List<String> propertiesSortedList) {
        this.propertiesSortedList = propertiesSortedList;
    }

    public Map<Integer, PropertyColumnEntry> getColumnMap() {
        return columnMap;
    }

    public boolean checkSensorSystemCompleteSchemaChange(Long projectId) throws Exception{
        while (true){
            int c=0;
            List<EventEntry> eventEntryList=metaClient.queryAllEventByProjectId(projectId);
            if(CollectionUtils.isEmpty(eventEntryList)){
                throw new IllegalArgumentException("this project has no any event,projectId="+projectId);
            }
            eventEntryList.stream().forEach(x->eventMap.put(x.getName(),x));
            List<PropertyEntry> propertyEntryList=metaClient.queryAllPropertiesByProjectId(projectId,TableType.EVENT.getIndex());
            if(CollectionUtils.isEmpty(propertyEntryList)){
                throw new IllegalArgumentException("this project has no any property,projectId="+projectId);
            }
            propertyEntryList.stream().forEach(x->proMap.put(x.getName(),x));
            List<Integer> ids=propertyEntryList.stream().map(x->x.getId()).collect(Collectors.toList());
            List<PropertyColumnEntry>  columnEntries=metaClient.queryAllPropertiesColumnByPropertyId(ids);
            if(CollectionUtils.isEmpty(columnEntries)){
                throw new IllegalArgumentException("this project has no any property column,projectId="+projectId);
            }
            columnEntries.stream().forEach(x->columnMap.put(x.getProperty_define_id(),x));
            proMap.keySet().stream().forEach(x->proColumnMap.put(x,columnMap.get(proMap.get(x).getId())));
            propertiesSortedList.clear();
            propertiesSortedList.addAll(proMap.keySet());
            propertiesSortedList.sort(new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    return o1.compareTo(o2);
                }
            });
            long count=columnEntries.stream().filter(x->StringUtils.isEmpty(x.getColumn_name())).count()
                    +propertyEntryList.stream().filter(x->StringUtils.isEmpty(x.getView_column_name())).count();
            if(count>0){
                Thread.sleep(500);
                c++;
                if(c>=20){
                    throw new IllegalArgumentException("wait sensor system to complete schema change for 10s,but not completed");
                }
                continue;
            }else {
                logger.info("sensor system has completed schema change,wait loop="+c);
                return true;
            }
        }
    }


    public boolean checkClickHouseSchemaNeedChange(Long projectId) throws Exception{
        List<PropertyEntry> propertyEntryList=metaClient.queryAllPropertiesByProjectId(projectId,TableType.EVENT.getIndex());
        if(CollectionUtils.isEmpty(propertyEntryList)){
            throw new IllegalArgumentException("this project has no any property,projectId="+projectId);
        }
        List<Integer> ids=propertyEntryList.stream().map(x->x.getId()).collect(Collectors.toList());
        List<PropertyColumnEntry> columnEntries=metaClient.queryAllPropertiesColumnByPropertyId(ids);
        if(CollectionUtils.isEmpty(columnEntries)){
            throw new IllegalArgumentException("this project has no any property column,projectId="+projectId);
        }
        Map<String,String> columnMap=metaClient.queryClickHouseSchema(projectId);
        List<PropertyColumnEntry> needAddColumns=columnEntries.stream().filter(x->!columnMap.containsKey(x.getColumn_name())).collect(Collectors.toList());
        Boolean exist=WormholeZkClient.checkExist(paramUtils.getZkAddress(),paramUtils.getZkFullPath());
        if(!exist){
            WormholeZkClient.createAndSetData(paramUtils.getZkAddress(),paramUtils.getZkFullPath(),"1");
        }

        //update namespace
        byte[] bytes=WormholeZkClient.getData(paramUtils.getZkAddress(),paramUtils.getZkFullPath());
        Integer ver=Integer.valueOf(new String(bytes));
        if(!needAddColumns.isEmpty()){
            metaClient.changeClickHouseSchema(needAddColumns);
            ver++;
            WormholeZkClient.createAndSetData(paramUtils.getZkAddress(),paramUtils.getZkFullPath(),String.valueOf(ver));
            logger.info("schema change!!!!!!! needAddColumns is {}, current version is {}", needAddColumns, ver);
        }
        List<String> ns= Lists.newArrayList(Splitter.on(".").split(paramUtils.getNameSpace()).iterator());
        ns.set(4,String.valueOf(ver));
        paramUtils.setNameSpace(Joiner.on(".").join(ns));

        return true;
    }


    public boolean checkClickHouseInitTable(Long projectId) throws Exception {
        return metaClient.checkAndCreateClickHouseTable();
    }



    public boolean initProject() throws Exception{
        this.checkClickHouseInitTable(this.paramUtils.getMyProjectId());
        this.checkSensorSystemCompleteSchemaChange(this.paramUtils.getMyProjectId());
        this.checkClickHouseSchemaNeedChange(this.paramUtils.getMyProjectId());
        return true;
    }





    public enum KafkaOriginColumn{
        _track_id,
        time,
        type,
        distinct_id,
        lib,
        event,
        properties,
        _flush_time,
        map_id,
        user_id,
        recv_time,
        extractor,
        project_id,
        project,
        ver,
        dtk
    }
}
