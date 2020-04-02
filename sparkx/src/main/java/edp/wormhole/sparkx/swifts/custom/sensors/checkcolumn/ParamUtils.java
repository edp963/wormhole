package edp.wormhole.sparkx.swifts.custom.sensors.checkcolumn;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Preconditions;
import edp.wormhole.sparkx.swifts.custom.sensors.DataTypeSensorToCK;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author daemon
 * @Date 19/11/14 16:00
 * To change this template use File | Settings | File Templates.
 */
public final class ParamUtils implements Serializable {

    private  ParamUtils.ParamEntry entry=null;

    private String zkAddress=null;

    private String zkPrefixPath=null;

    private String zkFullPath=null;

    private String nameSpace=null;

    private Map<Integer,Object> dataTypeConstMap=new HashMap();

    public ParamUtils (String json,String zkAddress,String zkPrefixPath,String nameSpace){
        Preconditions.checkNotNull(zkAddress,"param [zkAddress] must not be null");
        Preconditions.checkNotNull(zkPrefixPath,"param [zkPath] must not be null");
        parseParam(json.replaceAll("\\\\",""));
        initDataTypeConstMap();
        this.zkAddress=zkAddress;
        this.zkPrefixPath=zkPrefixPath;
        this.zkFullPath=this.zkPrefixPath+"/"+String.valueOf(this.getEntry().getProjectId());
        this.nameSpace=nameSpace;
    }

    private void initDataTypeConstMap(){
        for(int i = 0; i< DataTypeSensorToCK.values().length-1; i++){
            DataTypeSensorToCK dt= DataTypeSensorToCK.values()[i];
            if(i>this.getEntry().getDataTypeConst().size()-1){
                dataTypeConstMap.put(dt.getIndex(),null);
                continue;
            }
            String s=this.getEntry().getDataTypeConst().get(i);
            if("NULL".equalsIgnoreCase(s)){
                dataTypeConstMap.put(dt.getIndex(),null);
                continue;
            }
            switch (dt){
                case NUMBER:
                    dataTypeConstMap.put(dt.getIndex(),Long.valueOf(s));
                    break;
                case STRING:
                    dataTypeConstMap.put(dt.getIndex(),s);
                    break;
                case LIST:
                    dataTypeConstMap.put(dt.getIndex(),s);
                    break;
                case DATE:
                    dataTypeConstMap.put(dt.getIndex(),Long.valueOf(s));
                    break;
                case DATETIME:
                    dataTypeConstMap.put(dt.getIndex(),Long.valueOf(s));
                    break;
                case BOOL:
                    dataTypeConstMap.put(dt.getIndex(),Integer.valueOf(s));
                    break;
                case UNKNOWN:
                    dataTypeConstMap.put(dt.getIndex(),null);
                    break;
            }
        }
    }

    public  void  parseParam(String json){
        this.entry=JSON.parseObject(json,ParamEntry.class);
        Preconditions.checkNotNull(entry,"param must not be null");
        Preconditions.checkNotNull(entry.getProjectId(),"param [projectId] must not be null");
        Preconditions.checkNotNull(entry.getMysqlConnUrl(),"param [mysqlConnUrl] must not be null");
        Preconditions.checkNotNull(entry.getMysqlUser(),"param [mysqlUser] must not be null");
        Preconditions.checkNotNull(entry.getMysqlPassword(),"param [mysqlPassword] must not be null");
        Preconditions.checkNotNull(entry.getMysqlDatabase(),"param [mysqlDatabase] must not be null");
        Preconditions.checkNotNull(entry.getDuration(),"param [duration] must not be null");
        Preconditions.checkNotNull(entry.getDataTypeConst(),"param [dataTypeConst] must not be null");
        Preconditions.checkArgument(entry.getDataTypeConst().size()== DataTypeSensorToCK.values().length-1,"param [dataTypeConst] length must  be 6");

    }

    public  Long getMyProjectId(){
        return entry.getProjectId();
    }

    public ParamEntry getEntry() {
        return entry;
    }

    public String getZkFullPath() {
        return zkFullPath;
    }

    public String getZkAddress() {
        return zkAddress;
    }

    public String getNameSpace() {
        return nameSpace;
    }

    public void setNameSpace(String nameSpace) {
        this.nameSpace = nameSpace;
    }

    public String getZkPrefixPath() {
        return zkPrefixPath;
    }

    public Map<Integer, Object> getDataTypeConstMap() {
        return dataTypeConstMap;
    }

    public void setDataTypeConstMap(Map<Integer, Object> dataTypeConstMap) {
        this.dataTypeConstMap = dataTypeConstMap;
    }

    public static class ParamEntry implements Serializable {
        private Long projectId;
        private String mysqlConnUrl;
        private String mysqlUser;
        private String mysqlPassword;
        private String mysqlDatabase;
        private Long   duration;
        private List<String> dataTypeConst;


        public Long getProjectId() {
            return projectId;
        }

        public void setProjectId(Long projectId) {
            this.projectId = projectId;
        }

        public String getMysqlConnUrl() {
            return mysqlConnUrl;
        }

        public void setMysqlConnUrl(String mysqlConnUrl) {
            this.mysqlConnUrl = mysqlConnUrl;
        }

        public String getMysqlUser() {
            return mysqlUser;
        }

        public void setMysqlUser(String mysqlUser) {
            this.mysqlUser = mysqlUser;
        }

        public String getMysqlPassword() {
            return mysqlPassword;
        }

        public void setMysqlPassword(String mysqlPassword) {
            this.mysqlPassword = mysqlPassword;
        }

        public String getMysqlDatabase() {
            return mysqlDatabase;
        }

        public void setMysqlDatabase(String mysqlDatabase) {
            this.mysqlDatabase = mysqlDatabase;
        }


        public Long getDuration() {
            return duration;
        }

        public void setDuration(Long duration) {
            this.duration = duration;
        }

        public List<String> getDataTypeConst() {
            return dataTypeConst;
        }

        public void setDataTypeConst(List<String> dataTypeConst) {
            this.dataTypeConst = dataTypeConst;
        }

        @Override
        public String toString() {
            return "ParamEntry{" +
                    "projectId=" + projectId +
                    ", mysqlConnUrl='" + mysqlConnUrl + '\'' +
                    ", mysqlUser='" + mysqlUser + '\'' +
                    ", mysqlPassword='" + mysqlPassword + '\'' +
                    ", mysqlDatabase='" + mysqlDatabase + '\'' +
                    ", duration=" + duration +
                    ", dataTypeConst=" + dataTypeConst +
                    '}';
        }
    }
}
