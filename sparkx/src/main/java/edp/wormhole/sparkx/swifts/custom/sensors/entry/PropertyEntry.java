package edp.wormhole.sparkx.swifts.custom.sensors.entry;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author daemon
 * @Date 19/11/15 10:54
 * To change this template use File | Settings | File Templates.
 */
public class PropertyEntry implements Serializable {

    private Integer id;

    private Integer table_type;

    private String name;

    private String cname;

    private Integer data_type;

    private String view_column_name;

    private Integer cardinality;

    private Integer is_dimension;

    private Integer is_measure;

    private Integer is_in_use;

    private Integer is_load;

    private Integer is_truncated;

    private String comment;

    private String unit;

    private Integer has_dict;

    private Integer project_id;

    private String default_value;

    private Integer type_flexible;

    private Date update_time;


    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getTable_type() {
        return table_type;
    }

    public void setTable_type(Integer table_type) {
        this.table_type = table_type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCname() {
        return cname;
    }

    public void setCname(String cname) {
        this.cname = cname;
    }

    public Integer getData_type() {
        return data_type;
    }

    public void setData_type(Integer data_type) {
        this.data_type = data_type;
    }

    public String getView_column_name() {
        return view_column_name;
    }

    public void setView_column_name(String view_column_name) {
        this.view_column_name = view_column_name;
    }

    public Integer getCardinality() {
        return cardinality;
    }

    public void setCardinality(Integer cardinality) {
        this.cardinality = cardinality;
    }

    public Integer getIs_dimension() {
        return is_dimension;
    }

    public void setIs_dimension(Integer is_dimension) {
        this.is_dimension = is_dimension;
    }

    public Integer getIs_measure() {
        return is_measure;
    }

    public void setIs_measure(Integer is_measure) {
        this.is_measure = is_measure;
    }

    public Integer getIs_in_use() {
        return is_in_use;
    }

    public void setIs_in_use(Integer is_in_use) {
        this.is_in_use = is_in_use;
    }

    public Integer getIs_load() {
        return is_load;
    }

    public void setIs_load(Integer is_load) {
        this.is_load = is_load;
    }

    public Integer getIs_truncated() {
        return is_truncated;
    }

    public void setIs_truncated(Integer is_truncated) {
        this.is_truncated = is_truncated;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public Integer getHas_dict() {
        return has_dict;
    }

    public void setHas_dict(Integer has_dict) {
        this.has_dict = has_dict;
    }

    public Integer getProject_id() {
        return project_id;
    }

    public void setProject_id(Integer project_id) {
        this.project_id = project_id;
    }

    public String getDefault_value() {
        return default_value;
    }

    public void setDefault_value(String default_value) {
        this.default_value = default_value;
    }

    public Integer getType_flexible() {
        return type_flexible;
    }

    public void setType_flexible(Integer type_flexible) {
        this.type_flexible = type_flexible;
    }

    public Date getUpdate_time() {
        return update_time;
    }

    public void setUpdate_time(Date update_time) {
        this.update_time = update_time;
    }
}
