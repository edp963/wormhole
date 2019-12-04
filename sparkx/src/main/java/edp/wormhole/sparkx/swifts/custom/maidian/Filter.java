package edp.wormhole.sparkx.swifts.custom.maidian;

import java.io.Serializable;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author daemon
 * @Date 19/7/5 11:13
 * To change this template use File | Settings | File Templates.
 */
public class Filter implements Serializable {

    private String field;

    private String function;

    private List<String> param;

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getFunction() {
        return function;
    }

    public void setFunction(String function) {
        this.function = function;
    }

    public List<String> getParam() {
        return param;
    }

    public void setParam(List<String> param) {
        this.param = param;
    }
}
