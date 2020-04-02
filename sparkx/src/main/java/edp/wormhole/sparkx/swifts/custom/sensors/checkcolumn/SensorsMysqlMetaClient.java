package edp.wormhole.sparkx.swifts.custom.sensors.checkcolumn;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import edp.wormhole.sparkx.swifts.custom.sensors.entry.EventEntry;
import edp.wormhole.sparkx.swifts.custom.sensors.entry.PropertyColumnEntry;
import edp.wormhole.sparkx.swifts.custom.sensors.entry.PropertyEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.Serializable;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author daemon
 * @Date 19/11/15 11:15
 * To change this template use File | Settings | File Templates.
 */
public class SensorsMysqlMetaClient implements Serializable {

    private Connection mysqlConn;

    private ParamUtils paramUtils;


    private static final Logger logger=LoggerFactory.getLogger(SensorsMysqlMetaClient.class);


    public SensorsMysqlMetaClient(ParamUtils paramUtils) throws  Exception{
        this.paramUtils=paramUtils;
        Class.forName("com.mysql.jdbc.Driver");
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        if (drivers.hasMoreElements()) {
            while (true) {
                DriverManager.deregisterDriver(drivers.nextElement());
                if (!drivers.hasMoreElements()) {
                    break;
                }
            }
        }
        DriverManager.registerDriver(new com.mysql.jdbc.Driver());
        mysqlConn = DriverManager.getConnection(paramUtils.getEntry().getMysqlConnUrl(),paramUtils.getEntry().getMysqlUser(),paramUtils.getEntry().getMysqlPassword());
    }

    public List<PropertyEntry> queryAllPropertiesByProjectId(Long projectId,Integer tableType) throws Exception{
        PreparedStatement ps=null;
        ResultSet rs=null;
        try{
            List<String> selectColumns=getSelectColumn(PropertyEntry.class);
            StringBuilder sb=new StringBuilder();
            sb.append("select ").append(Joiner.on(",").join(selectColumns)).append(" from property_define where project_id=? and table_type=?");
            logger.info("properties sql="+sb.toString());
            ps=mysqlConn.prepareStatement(sb.toString());
            ps.setLong(1,projectId);
            ps.setInt(2,tableType);
            rs=ps.executeQuery();
            List<PropertyEntry> entries=getTList(rs,PropertyEntry.class);
            return entries;
        }finally {
            releaseConnection(null,ps,rs);
        }
    }


    public List<PropertyColumnEntry> queryAllPropertiesColumnByPropertyId(List<Integer> propertyIds) throws Exception{
        PreparedStatement ps=null;
        ResultSet rs=null;
        try{
            List<String> selectColumns=getSelectColumn(PropertyColumnEntry.class);
            StringBuilder sb=new StringBuilder();
            sb.append("select ").append(Joiner.on(",").join(selectColumns)).append(" from property_column where property_define_id in (").append(Joiner.on(',').join(propertyIds)).append(");");
            logger.info("column sql="+sb.toString());
            ps=mysqlConn.prepareStatement(sb.toString());
            rs=ps.executeQuery();
            List<PropertyColumnEntry> entries=getTList(rs,PropertyColumnEntry.class);
            return entries;
        }finally {
            releaseConnection(null,ps,rs);
        }
    }

    public List<EventEntry> queryAllEventByProjectId(Long projectId) throws Exception{
        PreparedStatement ps=null;
        ResultSet rs=null;
        try{
            List<String> selectColumns=getSelectColumn(EventEntry.class);
            StringBuilder sb=new StringBuilder();
            sb.append("select ").append(Joiner.on(",").join(selectColumns)).append(" from event_define where project_id=? and `virtual`=0");
            logger.info(" event sql="+sb.toString());
            ps=mysqlConn.prepareStatement(sb.toString());
            ps.setLong(1,projectId);
            rs=ps.executeQuery();
            List<EventEntry> entries=getTList(rs,EventEntry.class);
            return entries;
        }finally {
            releaseConnection(null,ps,rs);
        }
    }



    public void destroy(){
        releaseConnection(mysqlConn,null,null);
    }

    protected List<String> getSelectColumn(Class clazz){
        if(clazz==null){
            throw new IllegalArgumentException("parameter clazz must not be null");
        }
        Field[] fields=clazz.getDeclaredFields();
        if(fields==null || fields.length==0){
            throw new IllegalArgumentException("parameter clazz has no declared field");
        }
        List<String> columns= Lists.newArrayList();
        for(Field f:fields){
            columns.add("`"+f.getName()+"`");
        }
        return columns;
    }

    protected <T> List<T> getTList(ResultSet rs,Class clazz) throws Exception{
        if(clazz==null){
            throw new IllegalArgumentException("parameter clazz must not be null");
        }
        Field[] fields=clazz.getDeclaredFields();
        if(fields==null || fields.length==0){
            throw new IllegalArgumentException("parameter clazz has no declared field");
        }
        List<T> result=Lists.newArrayList();
        Map<String,Integer> colMap=Maps.newHashMap();
        for(int i=1;i<=rs.getMetaData().getColumnCount();i++){
            colMap.put(rs.getMetaData().getColumnName(i),i);
        }
        while (rs.next()){
            Object o=clazz.newInstance();
            for(Field f:fields){
                f.setAccessible(true);
                Object value=rs.getObject(f.getName());
                if(value!=null){
                    String cla=rs.getMetaData().getColumnClassName(colMap.get(f.getName()));
                    switch (cla){
                        case "java.lang.Integer":
                        case "java.lang.String":
                            f.set(o,value);
                            break;
                        case  "java.sql.Timestamp":
                        case "java.sql.Date":
                        case "java.sql.Time":
                            f.set(o,(java.util.Date)value);
                            break;
                        case "java.lang.Boolean" :
                            f.set(o,(Boolean)value==true?1:0);
                            break;
                    }
                }
            }
            result.add((T)o);
        }
        return result;
    }

    public static void releaseConnection(Connection con, Statement stat, ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException e) {
            logger.error("关闭ResultSet出现异常:", e);
        } finally {
            try {
                if (stat != null){
                    stat.close();
                }
            } catch (SQLException e) {
                logger.error("关闭Statement出现异常:", e);
            } finally {
                if (con != null) {
                    try {
                        con.close();
                    } catch (SQLException e) {
                        logger.error("关闭Connection出现异常:", e);
                    }
                }
            }
        }
    }
}
