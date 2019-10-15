package edp.wormhole.sparkx.swifts.custom.maidian;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.log4j.Logger;

import java.util.Map;

public class ConditionParse {

    static Logger logger = Logger.getLogger(ConditionParse.class.getName());

    public String getWhereSqlByNestCondition(JSONObject conditions, Map<String,DataType> tableFieldMap) {
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
                whereSql = function.getFieldWhereClause(filter.getField(),tableFieldMap.get(filter.getField().toLowerCase()),filter.getParam());
                break;
            case "and":
                JSONArray filterArrayAnd = JSON.parseArray(conditions.getString("filter"));
                String leftWhereAnd = getWhereSqlByNestCondition(filterArrayAnd.getJSONObject(0), tableFieldMap);
                String rightWhereAnd = getWhereSqlByNestCondition(filterArrayAnd.getJSONObject(1), tableFieldMap);
                whereSql = "(" + leftWhereAnd +") and (" + rightWhereAnd +")";
                break;
            case "or":
                JSONArray filterArrayOr = JSON.parseArray(conditions.getString("filter"));
                String leftWhereOr = getWhereSqlByNestCondition(filterArrayOr.getJSONObject(0), tableFieldMap);
                String rightWhereOr = getWhereSqlByNestCondition(filterArrayOr.getJSONObject(1), tableFieldMap);
                whereSql = "(" + leftWhereOr +") and (" + rightWhereOr +")";
                break;
            default:
                break;
        }
        return whereSql;
    }
}
