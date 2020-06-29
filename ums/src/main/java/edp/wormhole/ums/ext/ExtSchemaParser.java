package edp.wormhole.ums.ext;

import com.alibaba.fastjson.JSON;
import edp.wormhole.ums.UmsField;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Suxy
 * @date 2020/5/20
 * @description file description
 */
@SuppressWarnings("unchecked")
public class ExtSchemaParser {

    private static final String TYPE_CSV = "csv";
    private static final String TYPE_JSON = "json";
    private static final String DELIMITER = "$";
    private static final String DELIMITER_REX = "\\" + DELIMITER;

    private ExtSchemaParser() {

    }

    public static String extFormat(String raw, Seq<UmsField> fields, ExtSchemaConfig extSchemaConfig) {
        Map<String, Object> target = new LinkedHashMap<>();
        if (TYPE_CSV.equals(extSchemaConfig.type())) {
            int fieldIndex = 0;
            String [] tokens = raw.split(extSchemaConfig.field_delimiter(), -1);
            for (int i = 0; i < tokens.length; i ++) {
                String field = fields.apply(fieldIndex).name();
                if (field.contains(DELIMITER)) {
                    List<String> tmpFields = new ArrayList<>();
                    String prefix = field.split(DELIMITER_REX)[0];
                    for (int j = i; j < fields.size(); j ++) {
                        if (fields.apply(j).name().startsWith(prefix + DELIMITER)) {
                            tmpFields.add(fields.apply(j).name());
                            fieldIndex ++;
                        } else {
                            break;
                        }
                    }
                    for (ExtSchemaFieldConfig item : extSchemaConfig.obj_fields()) {
                        if (prefix.equals(item.name())) {
                            recursiveFormat(item, prefix, tmpFields, tokens[i], target);
                            break;
                        }
                    }
                } else {
                    target.put(field, tokens[i]);
                    fieldIndex ++;
                }
                if (fieldIndex >= fields.length()) {
                    break;
                }
            }
        } else if (TYPE_JSON.equals(extSchemaConfig.type())) {
            LinkedHashMap<String, Object> tokenMap = JSON.parseObject(raw, LinkedHashMap.class);
            tokenMap.forEach((key, val) -> {
                for (int i = 0; i < fields.size(); i ++) {
                    String field = fields.apply(i).name();
                    if (field.equals(key)) {
                        target.put(key, val);
                        break;
                    } else if (field.startsWith(key + DELIMITER)) {
                        List<String> tmpFields = new ArrayList<>();
                        for (int j = i; j < fields.size(); j ++) {
                            if (fields.apply(j).name().startsWith(key + DELIMITER)) {
                                tmpFields.add(fields.apply(j).name());
                            } else {
                                break;
                            }
                        }
                        for (ExtSchemaFieldConfig item : extSchemaConfig.obj_fields()) {
                            if (key.equals(item.name())) {
                                if (TYPE_CSV.equals(item.type())) {
                                    recursiveFormat(item, key, tmpFields, val.toString(), target);
                                } else {
                                    recursiveFormat(item, key, tmpFields, JSON.toJSONString(val), target);
                                }
                                break;
                            }
                        }
                        break;
                    }
                }
            });
        } else {
            return raw;
        }
        return JSON.toJSONString(target);
    }

    private static void recursiveFormat(ExtSchemaFieldConfig extSchemaFieldConfig, String prefix, List<String> tmpFields, String token, Map<String, Object> target) {
        List<String> clearFields = new ArrayList<>();
        Map<String, Object> clearTokenMap = new LinkedHashMap<>();
        if (TYPE_CSV.equals(extSchemaFieldConfig.type())) {
            String [] tokens = token.split(extSchemaFieldConfig.field_delimiter(), -1);
            for (int i = 0; i < tmpFields.size(); i ++) {
                String field = tmpFields.get(i).substring((prefix + DELIMITER).length());
                if (field.contains(DELIMITER)) {
                    clearFields.add(prefix + DELIMITER + field);
                    String key = field.split(DELIMITER_REX)[0];
                    if (!clearTokenMap.containsKey(key)) {
                        clearTokenMap.put(key, tokens[i]);
                    }
                } else {
                    target.put(prefix + DELIMITER + field, tokens[i]);
                }
            }
        } else if (TYPE_JSON.equals(extSchemaFieldConfig.type())) {
            LinkedHashMap<String, Object> tokenMap = JSON.parseObject(token, LinkedHashMap.class);
            for (String field : tmpFields) {
                field = field.substring((prefix + DELIMITER).length());
                if (field.contains(DELIMITER)) {
                    clearFields.add(prefix + DELIMITER + field);
                    String key = field.split(DELIMITER_REX)[0];
                    clearTokenMap.put(key, tokenMap.get(key));
                } else {
                    target.put(prefix + DELIMITER + field, tokenMap.get(field));
                }
            }
        }
        //解析子集
        for (ExtSchemaFieldConfig item : extSchemaFieldConfig.obj_fields()) {
            if (clearTokenMap.get(item.name()) != null) {
                token = JSON.toJSONString(clearTokenMap.get(item.name()));
                tmpFields = clearFields.stream().filter(i -> i.startsWith(prefix + DELIMITER + item.name())).collect(Collectors.toList());
                recursiveFormat(item, prefix + DELIMITER + item.name(), tmpFields, token, target);
            }
        }
    }

}
