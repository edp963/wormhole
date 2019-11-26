package edp.wormhole.sparkx.swifts.custom.sensors.entry;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author daemon
 * @Date 19/11/15 10:55
 * To change this template use File | Settings | File Templates.
 */
public class PropertyColumnEntry implements Serializable {

    private Integer id;

    private Integer property_define_id;

    private Integer data_type;

    private Integer true_list;

    private String column_name;

    private Integer is_query_active;

    private Date update_time;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getProperty_define_id() {
        return property_define_id;
    }

    public void setProperty_define_id(Integer property_define_id) {
        this.property_define_id = property_define_id;
    }

    public Integer getData_type() {
        return data_type;
    }

    public void setData_type(Integer data_type) {
        this.data_type = data_type;
    }

    public Integer getTrue_list() {
        return true_list;
    }

    public void setTrue_list(Integer true_list) {
        this.true_list = true_list;
    }

    public String getColumn_name() {
        return column_name;
    }

    public void setColumn_name(String column_name) {
        this.column_name = column_name;
    }

    public Integer getIs_query_active() {
        return is_query_active;
    }

    public void setIs_query_active(Integer is_query_active) {
        this.is_query_active = is_query_active;
    }

    public Date getUpdate_time() {
        return update_time;
    }

    public void setUpdate_time(Date update_time) {
        this.update_time = update_time;
    }
}