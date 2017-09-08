/*-
 * <<
 * wormhole
 * ==
 * Copyright (C) 2016 - 2017 EDP
 * ==
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * >>
 */


package edp.wormhole.ums;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import java.util.List;

public class GenerateJavaUms {
    private JSONObject getProtocol(String protocol) {
        String rst = protocol.trim().toLowerCase();
        String increment = UmsProtocolType.DATA_INCREMENT_DATA().toString();
        String initial = UmsProtocolType.DATA_INITIAL_DATA().toString();
        String batch = UmsProtocolType.DATA_BATCH_DATA().toString();
        assert (rst.equals(increment) || rst.equals(initial) || rst.equals(batch)) : "protocol can only be " + increment +", " +initial + " or " + batch;
        JSONObject proto = new JSONObject();
        proto.put("type", rst);
        return proto;
    }

    private String getNamespace(String namespace) {
        assert namespace.split("\\.").length == 7 : "namespace should be in 7 segments";
        return namespace.trim().toLowerCase();
    }

    private Object getFields(List<JavaUmsField> list) {
        return JSON.toJSON(list);
    }

    private JSONObject getSchema(Object fields, String namespace) {
        JSONObject schema = new JSONObject();
        schema.put("fields", fields);
        schema.put("namespace", namespace);
        return schema;
    }

    private Object getPayloads(List<JavaTuple> list) {
        return JSON.toJSON(list);
   }

    public String generateUms(String inputProtocol, String inputNamespace, List<JavaUmsField> inputFields, List<JavaTuple> inputPayload) {
        JSONObject protocol = getProtocol(inputProtocol);
        String namespace = getNamespace(inputNamespace);
        Object fields = getFields(inputFields);
        JSONObject schema = getSchema(fields, namespace);
        Object payload = getPayloads(inputPayload);
        JSONObject ums = new JSONObject();
        ums.put("protocol",protocol);
        ums.put("schema",schema);
        ums.put("payload",payload);
        return ums.toJSONString();
    }
}
