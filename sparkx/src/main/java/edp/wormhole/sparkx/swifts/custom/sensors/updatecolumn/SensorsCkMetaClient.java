package edp.wormhole.sparkx.swifts.custom.sensors.updatecolumn;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import edp.wormhole.sparkx.swifts.custom.sensors.DataTypeSensorToCK;
import edp.wormhole.sparkx.swifts.custom.sensors.entry.PropertyColumnEntry;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseDriver;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author daemon
 * @Date 19/11/15 11:15
 * To change this template use File | Settings | File Templates.
 */
public class SensorsCkMetaClient implements Serializable {

    private List<Connection> clickHouseConn;

    private ParamUtils paramUtils;

    private static final Logger logger=LoggerFactory.getLogger(SensorsCkMetaClient.class);

    public SensorsCkMetaClient(ParamUtils paramUtils) throws  Exception{
        this.paramUtils=paramUtils;
        ClickHouseDriver driver=new ClickHouseDriver();
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUser(paramUtils.getEntry().getClickHouseUser());
        properties.setPassword(paramUtils.getEntry().getClickHousePassword());
        List<String> urls=Lists.newArrayList(Splitter.on(",").split(paramUtils.getEntry().getClickHouseConnUrl()).iterator());
        clickHouseConn=Lists.newArrayList();
        for(String url:urls){
            Connection conn=driver.connect(url,properties);
            clickHouseConn.add(conn);
        }
    }

    public Map<String,String> queryClickHouseSchema() throws Exception{
        Statement ps=null;
        ResultSet rs=null;
        try{
            StringBuilder sb=new StringBuilder();
            sb.append("desc ").append(paramUtils.getEntry().getClickHouseDatabase()).append(".").append(paramUtils.getEntry().getClickHouseTableName()).append(";");
            ps=clickHouseConn.get(0).createStatement();
            rs=ps.executeQuery(sb.toString());
            Map<String,String> result= Maps.newHashMap();
            while (rs.next()){
                result.put(rs.getString("name"),rs.getString("type"));
            }
            return result;
        }finally {
            releaseConnection(null,ps,rs);
        }
    }

    public boolean checkAndCreateClickHouseTable() throws Exception{
        Statement ps=null;
        ResultSet rs=null;
        try{
            boolean database=false;
            StringBuilder sb=new StringBuilder();
            sb.append("select * from system.databases where name='").append(paramUtils.getEntry().getClickHouseDatabase()).append("' limit 1;");
            ps=clickHouseConn.get(0).createStatement();
            rs=ps.executeQuery(sb.toString());
            while (rs.next()){
                database=StringUtils.equalsIgnoreCase(rs.getString("name"),paramUtils.getEntry().getClickHouseDatabase());
                if(database){
                    break;
                }
            }
            releaseConnection(null,ps,rs);
            if(!database){
                StringBuilder s=new StringBuilder();
                s.append("create database IF NOT EXISTS ").append(paramUtils.getEntry().getClickHouseDatabase()).append(" on cluster  ").append(paramUtils.getEntry().getClickHouseCluster()).append(";");
                logger.info("will create Distributed database="+paramUtils.getEntry().getClickHouseDatabase()+",sql="+s.toString());
                ps=clickHouseConn.get(0).createStatement();
                ps.execute(s.toString());
                logger.info("created Distributed database="+paramUtils.getEntry().getClickHouseDatabase());
                releaseConnection(null,ps,null);
                StringBuilder l=new StringBuilder();
                l.append("create database IF NOT EXISTS ").append(paramUtils.getEntry().getClickHouseDatabase()).append(";");
                logger.info("will create local database="+paramUtils.getEntry().getClickHouseDatabase()+",sql="+l.toString());

                for(Connection conn:clickHouseConn){
                    ps=conn.createStatement();
                    ps.execute(l.toString());
                    releaseConnection(null,ps,null);
                    logger.info("created local database="+paramUtils.getEntry().getClickHouseDatabase());
                }
            }

        }catch (Exception e){
            logger.error("check and create database error,e=",e);
        }
        try{
            boolean table=false;
            StringBuilder sb=new StringBuilder();
            sb.append("select * from system.tables where database='").append(paramUtils.getEntry().getClickHouseDatabase()).append("' and name='").append(paramUtils.getEntry().getClickHouseTableName()).append("' limit 1;");
            ps=clickHouseConn.get(0).createStatement();
            rs=ps.executeQuery(sb.toString());
            while (rs.next()){
                table=StringUtils.equalsIgnoreCase(rs.getString("name"),paramUtils.getEntry().getClickHouseTableName());
                if(table){
                    break;
                }
            }
            releaseConnection(null,ps,rs);
            if(!table){
                StringBuilder s=new StringBuilder();
                s.append("create table IF NOT EXISTS ")
                        .append(paramUtils.getEntry().getClickHouseDatabase()).append(".").append(paramUtils.getEntry().getClickHouseTableName())
                        .append(" on cluster  ").append(paramUtils.getEntry().getClickHouseCluster())
                        .append(" (")
                        .append(" sampling_group Int32,")
                        .append(" user_id Int64,")
                        .append(" _offset Int64,")
                        .append(" event_id Int32,")
                        .append(" month_id Int32,")
                        .append(" week_id Int32,")
                        .append(" distinct_id String,")
                        .append(" event_bucket Int32,")
                        .append(" day Int32,")
                        .append(" time Int64,")
                        .append(" ums_ts_ DateTime,")
                        .append(" event_date Date,")
                        .append(" yx_user_id String")
                        .append(")engine= MergeTree() PARTITION by toYYYYMM(event_date) ORDER BY (event_date,event_id)");
                logger.info("will create Distributed table="+paramUtils.getEntry().getClickHouseTableName()+",sql="+s.toString());
                ps=clickHouseConn.get(0).createStatement();
                ps.execute(s.toString());
                releaseConnection(null,ps,null);
                logger.info("created Distributed table="+paramUtils.getEntry().getClickHouseTableName());

                StringBuilder l=new StringBuilder();
                l.append("create table IF NOT EXISTS ")
                        .append(paramUtils.getEntry().getClickHouseDatabase()).append(".").append(paramUtils.getEntry().getClickHouseTableName())
                        .append(" (")
                        .append(" sampling_group Int32,")
                        .append(" user_id Int64,")
                        .append(" _offset Int64,")
                        .append(" event_id Int32,")
                        .append(" month_id Int32,")
                        .append(" week_id Int32,")
                        .append(" distinct_id String,")
                        .append(" event_bucket Int32,")
                        .append(" day Int32,")
                        .append(" time Int64,")
                        .append(" ums_ts_ DateTime,")
                        .append(" event_date Date,")
                        .append(" yx_user_id String")
                        .append(")engine= Distributed(")
                        .append(paramUtils.getEntry().getClickHouseCluster()).append(",")
                        .append(paramUtils.getEntry().getClickHouseDatabase()).append(",")
                        .append(paramUtils.getEntry().getClickHouseTableName()).append(",")
                        .append("xxHash64(distinct_id))");
                logger.info("will create local table="+paramUtils.getEntry().getClickHouseTableName()+",sql="+l.toString());

                for(Connection conn:clickHouseConn){
                    ps=conn.createStatement();
                    ps.execute(l.toString());
                    releaseConnection(null,ps,null);
                    logger.info("created local table="+paramUtils.getEntry().getClickHouseTableName());
                }
            }
        }catch (Exception e){
            logger.error("check and create table error,e=",e);
        }
        return  true;
    }

    public boolean changeClickHouseSchema(Map<String, String> needAddColumn)throws Exception{
        List<String> columnSql=Lists.newArrayList();
        needAddColumn.keySet().forEach(columnName -> {
            columnSql.add(" ADD COLUMN IF NOT EXISTS "+columnName+" "+needAddColumn.get(columnName));
        });

        logger.info("needAddColumn="+columnSql.toString());
        Statement ps=null;
        try{
            StringBuilder disSql=new StringBuilder();
            disSql.append("alter table ")
                    .append(paramUtils.getEntry().getClickHouseDatabase()).append(".").append(paramUtils.getEntry().getClickHouseTableName())
                    .append(" on cluster ").append(paramUtils.getEntry().getClickHouseCluster())
                    .append(" ").append(Joiner.on(",").join(columnSql)).append(";");
            logger.info("add column Distributed table="+paramUtils.getEntry().getClickHouseTableName()+",sql="+disSql.toString());
            ps=clickHouseConn.get(0).createStatement();
            ps.execute(disSql.toString());
            logger.info("added column Distributed table="+paramUtils.getEntry().getClickHouseTableName());
        }catch (Exception e){
            logger.error("changeClickHouseSchema on cluster error,e=",e);
            return false;
        }finally {
            releaseConnection(null,ps,null);
        }
        StringBuilder localSql=new StringBuilder();
        localSql.append("alter table ")
                .append(paramUtils.getEntry().getClickHouseDatabase()).append(".").append(paramUtils.getEntry().getClickHouseTableName())
                .append(" ").append(Joiner.on(",").join(columnSql)).append(";");
        boolean rst=true;
        logger.info("add column local table="+paramUtils.getEntry().getClickHouseTableName()+",sql="+localSql.toString());
        for(Connection conn:clickHouseConn){
            try{
                ps=conn.createStatement();
                ps.execute(localSql.toString());
                logger.info("added column local table="+paramUtils.getEntry().getClickHouseTableName());
                rst=rst||true;
            }catch (Exception e){
                logger.error("changeClickHouseSchema on local error,e=",e);
                rst=rst||false;
            }finally {
                releaseConnection(null,ps,null);
            }
        }
        return rst;
    }


    public void destroy(){
        if(clickHouseConn!=null && clickHouseConn.size()>0){
            for(Connection conn:clickHouseConn){
                releaseConnection(conn,null,null);
            }
        }
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
