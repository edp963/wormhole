package edp.wormhole.sparkx.swifts.custom.sensors.entry;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author daemon
 * @Date 19/11/19 15:15
 * To change this template use File | Settings | File Templates.
 */
public class EventEntry implements Serializable {
    private Integer id;

    private String name;

    private String cname;

    private String comment;

    private Date create_time;

    private Integer visible;

    private Integer virtual;

    private Integer bucket_id;

    private String virtual_define;

    private Integer project_id;

    private Date update_time;

    private Integer user_id;

    private  Integer is_visualized;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
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

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public Date getCreate_time() {
        return create_time;
    }

    public void setCreate_time(Date create_time) {
        this.create_time = create_time;
    }

    public Integer getVisible() {
        return visible;
    }

    public void setVisible(Integer visible) {
        this.visible = visible;
    }

    public Integer getVirtual() {
        return virtual;
    }

    public void setVirtual(Integer virtual) {
        this.virtual = virtual;
    }

    public Integer getBucket_id() {
        return bucket_id;
    }

    public void setBucket_id(Integer bucket_id) {
        this.bucket_id = bucket_id;
    }

    public String getVirtual_define() {
        return virtual_define;
    }

    public void setVirtual_define(String virtual_define) {
        this.virtual_define = virtual_define;
    }

    public Integer getProject_id() {
        return project_id;
    }

    public void setProject_id(Integer project_id) {
        this.project_id = project_id;
    }

    public Date getUpdate_time() {
        return update_time;
    }

    public void setUpdate_time(Date update_time) {
        this.update_time = update_time;
    }

    public Integer getUser_id() {
        return user_id;
    }

    public void setUser_id(Integer user_id) {
        this.user_id = user_id;
    }

    public Integer getIs_visualized() {
        return is_visualized;
    }

    public void setIs_visualized(Integer is_visualized) {
        this.is_visualized = is_visualized;
    }
}