package edp.wormhole.sparkx.swifts.custom.sensors.updatecolumn;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Preconditions;

import java.io.Serializable;

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


    public ParamUtils(String json, String zkAddress, String zkPrefixPath, String nameSpace){
        Preconditions.checkNotNull(zkAddress,"param [zkAddress] must not be null");
        Preconditions.checkNotNull(zkPrefixPath,"param [zkPath] must not be null");
        parseParam(json.replaceAll("\\\\",""));
        this.zkAddress=zkAddress;
        this.zkPrefixPath=zkPrefixPath;
        this.zkFullPath=this.zkPrefixPath;
        this.nameSpace=nameSpace;
    }

    public void parseParam(String json){
        this.entry=JSON.parseObject(json,ParamEntry.class);
        Preconditions.checkNotNull(entry,"param must not be null");
        Preconditions.checkNotNull(entry.getClickHouseConnUrl(),"param [clickHouseConnUrl] must not be null");
        Preconditions.checkNotNull(entry.getClickHouseUser(),"param [clickHouseUser] must not be null");
        Preconditions.checkNotNull(entry.getClickHouseDatabase(),"param [clickHouseDatabase] must not be null");
        Preconditions.checkNotNull(entry.getClickHousePassword(),"param [clickHousePassword] must not be null");
        Preconditions.checkNotNull(entry.getClickHouseTableName(),"param [clickHouseTableName] must not be null");
        Preconditions.checkNotNull(entry.getClickHouseCluster(),"param [clickHouseCluster] must not be null");
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

    public static class ParamEntry implements Serializable {
        private String clickHouseConnUrl;
        private String clickHouseUser;
        private String clickHousePassword;
        private String clickHouseDatabase;
        private String clickHouseTableName;
        private String clickHouseCluster;

        public String getClickHouseCluster() {
            return clickHouseCluster;
        }

        public void setClickHouseCluster(String clickHouseCluster) {
            this.clickHouseCluster = clickHouseCluster;
        }

        public String getClickHouseTableName() {
            return clickHouseTableName;
        }

        public void setClickHouseTableName(String clickHouseTableName) {
            this.clickHouseTableName = clickHouseTableName;
        }


        public String getClickHouseConnUrl() {
            return clickHouseConnUrl;
        }

        public void setClickHouseConnUrl(String clickHouseConnUrl) {
            this.clickHouseConnUrl = clickHouseConnUrl;
        }

        public String getClickHouseUser() {
            return clickHouseUser;
        }

        public void setClickHouseUser(String clickHouseUser) {
            this.clickHouseUser = clickHouseUser;
        }

        public String getClickHousePassword() {
            return clickHousePassword;
        }

        public void setClickHousePassword(String clickHousePassword) {
            this.clickHousePassword = clickHousePassword;
        }

        public String getClickHouseDatabase() {
            return clickHouseDatabase;
        }

        public void setClickHouseDatabase(String clickHouseDatabase) {
            this.clickHouseDatabase = clickHouseDatabase;
        }

        @Override
        public String toString() {
            return "ParamEntry{" +
                    ", clickHouseConnUrl='" + clickHouseConnUrl + '\'' +
                    ", clickHouseUser='" + clickHouseUser + '\'' +
                    ", clickHousePassword='" + clickHousePassword + '\'' +
                    ", clickHouseDatabase='" + clickHouseDatabase + '\'' +
                    ", clickHouseTableName='" + clickHouseTableName + '\'' +
                    ", clickHouseCluster='" + clickHouseCluster + '\'' +
                    '}';
        }
    }
}
