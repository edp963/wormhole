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


//package edp.wormhole.udf;
//
//import com.sun.jmx.snmp.Timestamp;
//
//import java.math.BigDecimal;
//import java.util.ArrayList;
//import java.util.Date;
//import java.util.List;
//
//public class TestUdf {
//
//    public static void main(String[] s) {
//        try {
//            List<String> list = new ArrayList<String>();
//            list.add("edp.wormhole.udf.TestUdf");
//            UdfRegister.udfRegister(list, null);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    @WormholeUdfAnno
//    public int intTest(String in) {
//        return 1;
//    }
//
//    @WormholeUdfAnno
//    public Timestamp timestampTest(String in) {
//        return new Timestamp();
//    }
//
//    @WormholeUdfAnno
//    public Double double1Test(String in) {
//        return 1.1;
//    }
//
//    @WormholeUdfAnno
//    public Float float1Test(String in) {
//        return 1.1f;
//    }
//
//    @WormholeUdfAnno
//    public Long long1Test(String in) {
//        return 1l;
//    }
//
//    @WormholeUdfAnno
//    public Boolean boolean1Test(String in) {
//        return true;
//    }
//
//    @WormholeUdfAnno
//    public Integer integerTest(String in) {
//        return new Integer(1);
//    }
//
//    @WormholeUdfAnno
//    public Integer integerTest1(String in) {
//        return 1;
//    }
//
//    @WormholeUdfAnno
//    public String stringTest(int in, int s2) {
//        return "1";
//    }
//
//    @WormholeUdfAnno
//    public double doubleTest1(int in, int s2) {
//        return 1.1;
//    }
//
//    @WormholeUdfAnno
//    public BigDecimal bigdecimalTest1(int in, int s2) {
//        return new BigDecimal(1.1);
//    }
//
//    @WormholeUdfAnno
//    public float floatTest1(int in, int s2) {
//        return 1.1f;
//    }
//
//    @WormholeUdfAnno
//    public long longTest1(int in, int s2) {
//        return 1l;
//    }
//
//    @WormholeUdfAnno
//    public boolean booleanTest1(int in, int s2) {
//        return true;
//    }
//
//    @WormholeUdfAnno
//    public Byte[] byteTest1(int in, int s2) {
//        return new Byte[1];
//    }
//
//    @WormholeUdfAnno
//    public Date dateTest1(int in, int s2) {
//        return new Date();
//    }
//
//
//}
