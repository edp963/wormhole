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


package edp.wormhole.udf;

import javassist.*;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.UDFRegistration;
import org.apache.spark.sql.api.java.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.log4j.Logger;

import java.util.List;

import static org.apache.spark.sql.types.DataTypes.*;

public class UdfRegister {

    private Logger myLogger = Logger.getLogger("UdfRegister");

    public void udfRegister(List<String> classNames, SQLContext sqlContext) throws Exception {
        try {
            myLogger.info("start udfRegister");
            UDFRegistration udf = sqlContext.udf();
            ClassPool pool = ClassPool.getDefault();
            pool.appendClassPath(new LoaderClassPath(getClass().getClassLoader()));
            for (String srcClassName : classNames) {
                CtClass srcClass = pool.getCtClass(srcClassName);
                CtMethod[] srcCtMethods = srcClass.getDeclaredMethods();
                for (CtMethod srcCtMethod :srcCtMethods) {
                    myLogger.info("srcCtMethods.length:" + srcCtMethods.length);
                    String srcMethodName = srcCtMethod.getName();
                    if (srcMethodName.equals("call")) {
                        Object[] srcAnnos = srcCtMethod.getAnnotations();
                        for (Object srcAnno : srcAnnos) {
                            if (srcAnno instanceof WormholeUdfAnno) {
                                try {
                                    myLogger.info("srcClassName:" + srcClassName + ",srcMethodName:" + srcMethodName);
                                    String returnClassName = srcCtMethod.getReturnType().getName();
                                    if(!returnClassName.equals("java.lang.Object")) {
                                        int methodParamNum = srcCtMethod.getParameterTypes().length;
                                        DataType dataType = getSparkType(returnClassName);
                                        myLogger.info("returnClassName:" + returnClassName);
                                        String mn = ((WormholeUdfAnno) srcAnno).value();
                                        register(mn, methodParamNum, srcClass.toClass(), dataType, udf);
                                    }
                                } catch (Exception e1) {
                                    myLogger.info("register", e1);
                                }
                            }
                        }
                    }
                }
            }
            myLogger.info("finish udfRegister");
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

//    public void udfRegister1(List<String> classNames, SQLContext sqlContext) throws Exception {
//        myLogger.info("start udfRegister");
//        UDFRegistration udf = sqlContext.udf();
//        for (String srcClassName : classNames) {
//            ClassPool pool = ClassPool.getDefault();
//            pool.appendClassPath(new LoaderClassPath(getClass().getClassLoader()));
//            CtClass srcClass = pool.getCtClass(srcClassName);
//            CtMethod[] srcCtMethods = srcClass.getDeclaredMethods();
//
//            for (int i = 0; i < srcCtMethods.length; i++) {
//                try {
//                    CtMethod srcCtMethod = srcCtMethods[i];
//                    String srcMethodName = srcCtMethod.getName();
//                    String returnClassName = srcCtMethod.getReturnType().getName();
//
//                    CtClass destCtClass = pool.makeClass(srcClassName + "New" + i);
//                    CtClass copySign = pool.getCtClass("edp.wormhole.udf.EdpUdf1");
//
//                    int methodParamNum = srcCtMethod.getParameterTypes().length;
//                    for (int t = 0; t < methodParamNum; t++) {
//                        myLogger.info("ParameterTypes:" + srcCtMethod.getParameterTypes()[t]);
//                    }
//
//                    if (methodParamNum > 0) {
//                        Object[] srcAnnos = srcCtMethod.getAnnotations();
//                        for (Object srcAnno : srcAnnos) {
//                            if (srcAnno instanceof WormholeUdfAnno) {
//                                try {
////                                    if(returnClassName.equals("java.lang.Object")){
////                                        CtMethod destMethod1 = CtNewMethod.copy(srcCtMethod, "call", destCtClass, null);
////                                        destCtClass.addMethod(destMethod1);
////                                    }else {
//                                    DataType dataType = getSparkType(returnClassName);
////                                    SignatureAttribute.TypeParameter[] tp = new SignatureAttribute.TypeParameter[] { new SignatureAttribute.TypeParameter(Long.class.getName()),new SignatureAttribute.TypeParameter(Long.class.getName()) };
////                                    SignatureAttribute.ClassSignature cs = new SignatureAttribute.ClassSignature(tp);
//                                    CtClass udf1Class = pool.getCtClass("org.apache.spark.sql.api.java.UDF" + methodParamNum);
////                                    SignatureAttribute.ClassSignature cs = SignatureAttribute.toClassSignature(Descriptor.of(udf1Class) + "<" + Descriptor.of(Long.class.getName())+";"+Descriptor.of(Long.class.getName()) + ";>;");
//
////                                    udf1Class.setGenericSignature(cs.encode());
//                                    destCtClass.addInterface(udf1Class);
//                                    destCtClass.setGenericSignature(copySign.getGenericSignature());
//
//                                    CtMethod destMethod = CtNewMethod.copy(srcCtMethod, "call", destCtClass, null);
//                                    destCtClass.addMethod(destMethod);
//
//                                    CtConstructor ctor = new CtConstructor(new CtClass[0], destCtClass);
//                                    destCtClass.addConstructor(ctor);
//
////                                    destCtClass.addInterface(pool.getCtClass("java.io.Serializable"));
////                                    CtMethod[] oldCtM = destCtClass.getDeclaredMethods();
////                                    for(int j=0;j<oldCtM.length;j++){
////                                        if(oldCtM[j].getName().equals("call"))
////                                            destCtClass.removeMethod(oldCtM[j]);
////                                    }
////                                    SignatureAttribute.toTypeSignature()
//
//
////                                    SignatureAttribute.TypeParameter[] tp1 = new SignatureAttribute.TypeParameter[] { new SignatureAttribute.TypeParameter(Long.class.getName()),new SignatureAttribute.TypeParameter(Long.class.getName()) };
////                                    SignatureAttribute.ClassSignature cs1 = new SignatureAttribute.ClassSignature(tp1,null,udf1Class);
////                                    CtClass cscs = pool.getCtClass("edp.wormhole.udf.EdpUdf1");
////                                    destCtClass.setSuperclass(cscs);
////                                        destCtClass.setGenericSignature(cscs.getGenericSignature());
//
//                                    myLogger.info("register,destMethod:" + destCtClass.getName() + "," + destCtClass.toString() + ",dataType" + dataType.toString());
//                                    myLogger.info("Signature:" + destCtClass.getGenericSignature());
//                                    myLogger.info("destCtClass:" + destCtClass);
//                                    register(srcMethodName, methodParamNum, destCtClass.toClass(), dataType, udf);
////                                    }
//                                } catch (Exception e1) {
//                                    myLogger.error("register", e1);
//                                }
//                            }
//                        }
//                    }
//                } catch (Exception e) {
//                    e.printStackTrace();
//                    throw e;
//                }
//            }
//        }
//        myLogger.info("finish udfRegister");
//
//    }

    private void register(String srcMethodName, int methodParamNum, Class destClass, DataType dataType, UDFRegistration udf) throws Exception {
        switch (methodParamNum) {
            case 1:
                Object u1 = destClass.asSubclass(UDF1.class).newInstance();
                udf.register(srcMethodName, (UDF1) u1, dataType);
                break;
            case 2:
                Object u2 = destClass.asSubclass(UDF2.class).newInstance();
                udf.register(srcMethodName, (UDF2) u2, dataType);
                break;
            case 3:
                Object u3 = destClass.asSubclass(UDF3.class).newInstance();
                udf.register(srcMethodName, (UDF3) u3, dataType);
                break;
            case 4:
                Object u4 = destClass.asSubclass(UDF4.class).newInstance();
                udf.register(srcMethodName, (UDF4) u4, dataType);
                break;
            case 5:
                Object u5 = destClass.asSubclass(UDF5.class).newInstance();
                udf.register(srcMethodName, (UDF5) u5, dataType);
                break;
            case 6:
                Object u6 = destClass.asSubclass(UDF6.class).newInstance();
                udf.register(srcMethodName, (UDF6) u6, dataType);
                break;
            case 7:
                Object u7 = destClass.asSubclass(UDF7.class).newInstance();
                udf.register(srcMethodName, (UDF7) u7, dataType);
                break;
            case 8:
                Object u8 = destClass.asSubclass(UDF8.class).newInstance();
                udf.register(srcMethodName, (UDF8) u8, dataType);
                break;
            case 9:
                Object u9 = destClass.asSubclass(UDF9.class).newInstance();
                udf.register(srcMethodName, (UDF9) u9, dataType);
                break;
            case 10:
                Object u10 = destClass.asSubclass(UDF10.class).newInstance();
                udf.register(srcMethodName, (UDF10) u10, dataType);
                break;
            case 11:
                Object u11 = destClass.asSubclass(UDF11.class).newInstance();
                udf.register(srcMethodName, (UDF11) u11, dataType);
                break;
            case 12:
                Object u12 = destClass.asSubclass(UDF12.class).newInstance();
                udf.register(srcMethodName, (UDF12) u12, dataType);
                break;
            case 13:
                Object u13 = destClass.asSubclass(UDF13.class).newInstance();
                udf.register(srcMethodName, (UDF13) u13, dataType);
                break;
            case 14:
                Object u14 = destClass.asSubclass(UDF14.class).newInstance();
                udf.register(srcMethodName, (UDF14) u14, dataType);
                break;
            case 15:
                Object u15 = destClass.asSubclass(UDF15.class).newInstance();
                udf.register(srcMethodName, (UDF15) u15, dataType);
                break;
            case 16:
                Object u16 = destClass.asSubclass(UDF16.class).newInstance();
                udf.register(srcMethodName, (UDF16) u16, dataType);
                break;
            case 17:
                Object u17 = destClass.asSubclass(UDF17.class).newInstance();
                udf.register(srcMethodName, (UDF17) u17, dataType);
                break;
            case 18:
                Object u18 = destClass.asSubclass(UDF18.class).newInstance();
                udf.register(srcMethodName, (UDF18) u18, dataType);
                break;
            case 19:
                Object u19 = destClass.asSubclass(UDF19.class).newInstance();
                udf.register(srcMethodName, (UDF19) u19, dataType);
                break;
            case 20:
                Object u20 = destClass.asSubclass(UDF20.class).newInstance();
                udf.register(srcMethodName, (UDF20) u20, dataType);
                break;
            case 21:
                Object u21 = destClass.asSubclass(UDF21.class).newInstance();
                udf.register(srcMethodName, (UDF21) u21, dataType);
                break;
            case 22:
                Object u22 = destClass.asSubclass(UDF22.class).newInstance();
                udf.register(srcMethodName, (UDF22) u22, dataType);
                break;
            default:
                throw new Exception("too many args,max is 22");
        }

    }

    private DataType getSparkType(String returnClassName) throws Exception {
        switch (returnClassName) {
            case "int":
                return IntegerType;
            case "java.lang.Integer":
                return IntegerType;
            case "long":
                return LongType;
            case "java.lang.Long":
                return LongType;
            case "float":
                return FloatType;
            case "java.lang.Float":
                return FloatType;
            case "double":
                return DoubleType;
            case "java.lang.Double":
                return DoubleType;
            case "boolean":
                return BooleanType;
            case "java.lang.Boolean":
                return BooleanType;
            case "java.lang.String":
                return StringType;
            case "java.math.BigDecimal":
                return DecimalType.SYSTEM_DEFAULT();
            case "java.lang.Byte[]":
                return ByteType;
            case "java.util.Date":
                return DateType;
            case "java.sql.Date":
                return DateType;
            case "java.sql.Timestamp":
                return TimestampType;
            case "java.security.Timestamp":
                return TimestampType;
            case "com.sun.jmx.snmp.Timestamp":
                return TimestampType;
            default:
                throw new Exception("the type of " + returnClassName + " is not supported");
        }
    }
}
