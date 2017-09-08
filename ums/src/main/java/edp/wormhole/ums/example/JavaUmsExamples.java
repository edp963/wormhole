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


package edp.wormhole.ums.example;

import edp.wormhole.ums.*;
import java.util.Arrays;


public class JavaUmsExamples {
    public static void main(String[] args) {
        JavaUmsField ele1 = new JavaUmsField(UmsSysField.ID().toString(), JavaUmsFieldType.LONG, false);
        JavaUmsField ele2 = new JavaUmsField(UmsSysField.TS().toString(), JavaUmsFieldType.DATETIME, false);
        JavaUmsField ele3 = new JavaUmsField(UmsSysField.OP().toString(), JavaUmsFieldType.STRING, false);
        JavaUmsField ele4 = new JavaUmsField(UmsSysField.UID().toString(), JavaUmsFieldType.STRING, false);
        JavaUmsField ele5 = new JavaUmsField("key1", JavaUmsFieldType.INT, false);
        JavaUmsField ele6 = new JavaUmsField("key2", JavaUmsFieldType.STRING, false);
        JavaUmsField ele7 = new JavaUmsField("value1", JavaUmsFieldType.DECIMAL, true);
        JavaUmsField ele8 = new JavaUmsField("value2", JavaUmsFieldType.STRING);

        java.util.List<JavaUmsField> fields = Arrays.asList(ele1, ele2, ele3, ele4, ele5, ele6, ele7, ele8);
        GenerateJavaUms ums = new GenerateJavaUms();

        JavaTuple tuple1 = new JavaTuple(Arrays.asList("1", "2017-09-01 20:51:28.000", "i", "uid1", "1", "2", "3", "4"));
        JavaTuple tuple2 = new JavaTuple(Arrays.asList("2", "2017-09-01 20:52:28.000", "u", "uid2", "5", "6", "3", "4"));
        java.util.List<JavaTuple> payload = Arrays.asList(tuple1, tuple2);
        String rst = ums.generateUms(UmsProtocolType.DATA_INCREMENT_DATA().toString(), "mysql.mysql0.db.table.1.0.0", fields, payload);
        System.out.println(rst);
    }
}
