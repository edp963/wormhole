package edp.wormhole.flinkx.udf;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Map;

public class WhMapToString extends ScalarFunction {
    public String eval(Map<String, Integer> map) {
        JSONObject json = new JSONObject();
        if(map == null || map.size() == 0) {
            return json.toString();
        }
        for(Map.Entry<String, Integer> entity : map.entrySet()) {
            json.put(entity.getKey(), entity.getValue());
        }
        return json.toString();
    }
}
