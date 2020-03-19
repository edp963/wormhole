package edp.wormhole.sparkx.swifts.custom.maidian;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Joiner;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ConditionParse {

    static Logger logger = Logger.getLogger(ConditionParse.class.getName());

    public String getWhereSqlByNestCondition(JSONObject conditions, Map<String,DataType> tableFieldMap) throws Exception{
        String operator = conditions.getString("operator");
        String whereSql = "";
        switch (operator) {
            case "single":
                Filter filter = JSON.parseObject(conditions.getString("filter"), Filter.class);
                FilterFunction function = FilterFunction.getFilterFunction(filter.getFunction());
                if(null == function){
                    logger.error("can not find filter function " + filter.getFunction());
                    break;
                }
                whereSql = function.getFieldWhereClause(filter.getField(),tableFieldMap.get(filter.getField()),filter.getParam());
                break;
            case "and":
                JSONArray filterArrayAnd = JSON.parseArray(conditions.getString("filter"));
                List<String> whereArrayAnd = new ArrayList<>();
                for(int i=0; i<filterArrayAnd.size(); i++) {
                    String curWhereAnd = getWhereSqlByNestCondition(filterArrayAnd.getJSONObject(i), tableFieldMap);
                    whereArrayAnd.add(curWhereAnd);
                }
                whereSql = "(" + Joiner.on(") and (").join(whereArrayAnd) + ")";
                break;
            case "or":
                JSONArray filterArrayOr = JSON.parseArray(conditions.getString("filter"));
                List<String> whereArrayOr = new ArrayList<>();
                for(int i=0; i<filterArrayOr.size(); i++) {
                    String curWhereOr = getWhereSqlByNestCondition(filterArrayOr.getJSONObject(i), tableFieldMap);
                    whereArrayOr.add(curWhereOr);
                }
                whereSql = "(" + Joiner.on(") or (").join(whereArrayOr) + ")";
                break;
            default:
                break;
        }
        return whereSql;
    }
}
