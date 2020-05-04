package edp.wormhole.sparkx.swifts.custom.sensors;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import edp.wormhole.sparkx.swifts.custom.sensors.entry.PropertyColumnEntry;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.joda.time.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author daemon
 * @Date 19/11/19 11:16
 * To change this template use File | Settings | File Templates.
 */

public class ConvertUtils implements Serializable {

    private static Logger logger=LoggerFactory.getLogger(ConvertUtils.class);

//    public static List<String>  covert(List<String> contexts, Set<String> proKeySet, Map<String,EventEntry> eventMap, Map<String,PropertyEntry> propMap, Map<Integer,PropertyColumnEntry> columnMap){
//        List<String> newJsonList=Lists.newArrayList();
//        for(String context: contexts){
//            JSONObject o=JSON.parseObject(context);
//            try{
//                o.getLong(SchemaUtils.KafkaOriginColumn._trick_id.name());
//            }catch (Exception e){
//                logger.warn("kafka msg _trick_id_  is not number,set default -3L, context="+context);
//                o.put(SchemaUtils.KafkaOriginColumn._trick_id.name(),-3L);
//            }
//            JSONObject pro=o.getJSONObject(SchemaUtils.KafkaOriginColumn.properties.name());
//            pro.put("$kafka_offset",generateKafkaOffset(0L));
//            pro.put("$receive_time",o.getLong(SchemaUtils.KafkaOriginColumn.recv_time.name()));
//
//            JSONObject newObj=new JSONObject();
//            newObj.put("sampling_group",calcSamplingGroup(o.getLong(SchemaUtils.KafkaOriginColumn.user_id.name())));
//            newObj.put("user_id",o.getLong(SchemaUtils.KafkaOriginColumn.user_id.name()));
//            newObj.put("_offset",getOffset(o.getLong(SchemaUtils.KafkaOriginColumn._trick_id.name())));
//            DateTime dateTime = new DateTime(o.getLong(SchemaUtils.KafkaOriginColumn.time.name()));
//            newObj.put("day",calcDayId(dateTime.toLocalDateTime()));
//            newObj.put("week_id",calcWeekId(dateTime.toLocalDateTime()));
//            newObj.put("month_id",calcMonthId(dateTime.toLocalDateTime()));
//            newObj.put("distinct_id",o.getString(SchemaUtils.KafkaOriginColumn.distinct_id.name()));
//            EventEntry event=eventMap.get(o.getString(SchemaUtils.KafkaOriginColumn.event.name()));
//            newObj.put("event_id",event.getId());
//            newObj.put("event_bucket",event.getBucket_id());
//            newObj.put("time",o.getLong(SchemaUtils.KafkaOriginColumn.time.name()));
//            newObj.put("ums_ts_",dateTimeFormat(o.getLong(SchemaUtils.KafkaOriginColumn.time.name())));
//            newObj.put("event_date",dateFormat(o.getLong(SchemaUtils.KafkaOriginColumn.time.name())));
//            String userId=AESUtil.decrypt(o.getString(SchemaUtils.KafkaOriginColumn.distinct_id.name()));
//            newObj.put("yx_user_id",userId);
//            DataVersion version=DataVersion.indexOf(o.getInteger(SchemaUtils.KafkaOriginColumn.ver.name()));
//            JSONArray array=o.getJSONArray(SchemaUtils.KafkaOriginColumn.dtk.name());
//            Set<String> dtk= Sets.newHashSet();
//            if(array!=null && !array.isEmpty()){
//                dtk.addAll(Lists.newArrayList((Iterable<String>)array.iterator()));
//            }
//            for(String key:proKeySet){
//                if(key.equals("event_duration")){
//                    key="$event_duration";
//                }
//                PropertyEntry property=propMap.get(key);
//                PropertyColumnEntry column=columnMap.get(property.getId());
//                if(!pro.containsKey(key)){
//                    newObj.put(column.getColumn_name(),null);
//                    continue;
//                }
////            if(property.getIs_load()==0){
////                newObj.put(column.getColumn_name(),null);
////                continue;
////            }
//                Object value=pro.get(key);
//                if(value==null){
//                    newObj.put(column.getColumn_name(),null);
//                    continue;
//                }
////            DataType dataType=DataType.UNKNOWN;
////            if(version.getIndex()<DataVersion.VERSION2.getIndex()){
////                dataType=DataType.indexOf(property.getData_type());
////            }else {
////                dataType=judgeDataType(key,value,dtk);
////            }
////            if(dataType==DataType.UNKNOWN){
////                throw new IllegalArgumentException("can not get dataType,key="+key+",value="+value.toString());
////            }
//                switch (DataType.indexOf(column.getData_type())){
//                    case STRING:
//                        if(value instanceof String || value instanceof Integer || value instanceof Long || value instanceof Float || value instanceof Double ){
//                            newObj.put(column.getColumn_name(),String.valueOf(value));
//                        }else {
//                            throw new IllegalArgumentException("can not cast to STRING,key="+key+",value="+value.toString());
//                        }
//                        break;
//                    case NUMBER:
//                        Double double_=null;
//                        if (value instanceof Long) {
//                            double_ = Double.valueOf(((Long)value).longValue());
//                        } else if (value instanceof Integer) {
//                            double_ = Double.valueOf(((Integer)value).intValue());
//                        } else if (value instanceof Double) {
//                            double_ = (Double)value;
//                        } else if (value instanceof Float) {
//                            double_ = Double.valueOf(((Float)value).floatValue());
//                        } else if (value instanceof String) {
//                            double_ = Double.valueOf(String.valueOf(value));
//                        } else {
//                            throw new IllegalArgumentException("can not cast to NUMBER,key="+key+",value="+value.toString());
//                        }
//                        newObj.put(column.getColumn_name(),BigDecimal.valueOf(double_).multiply(NUMBER_FACTOR_BIG_DECIMAL).longValue());
//                        break;
//                    case DATE:
//                        if(value instanceof  String){
//                            try{
//                                newObj.put(column.getColumn_name(),Long.valueOf(dateStringToMsTimestamp(String.valueOf(value))));
//                            }catch (Exception e){
//                                logger.error("can not cast to DATE,key="+key+",value="+value.toString(),e);
//                                newObj.put(column.getColumn_name(),null);
//                            }
//
//                        }else {
//                            throw new IllegalArgumentException("can not cast to DATE,key="+key+",value="+value.toString());
//                        }
//                        break;
//                    case BOOL:
//                        Boolean bool=null;
//                        if (value instanceof Boolean) {
//                            bool = ((Boolean)value).booleanValue();
//                        } else if (value instanceof Long) {
//                            bool = (((Long)value).longValue() != 0L);
//                        } else if (value instanceof Integer) {
//                            bool = (((Integer)value).intValue() != 0);
//                        } else {
//                            throw new IllegalArgumentException("can not cast to BOOL,key="+key+",value="+value.toString());
//                        }
//                        newObj.put(column.getColumn_name(),bool?1:0);
//                        break;
//                    case LIST:
//                        if(value instanceof  JSONArray){
//                            JSONArray a=(JSONArray)value;
//                            if(a.isEmpty()){
//                                newObj.put(column.getColumn_name(),null);
//                            }else {
//                                List<String> result=Lists.newArrayList();
//                                Set<String>  result2=Sets.newHashSet();
//                                List<String> stringList=a.toJavaList(String.class);
//                                for(String x: stringList){
//                                    String str=normalize(x,false);
//                                    if(StringUtils.isNotBlank(str)){
//                                        result.add(str);
//                                        result2.add(str);
//                                    }
//                                }
//                                newObj.put(column.getColumn_name(),column.getTrue_list()==1?Joiner.on(",").join(result):Joiner.on(",").join(result2));
//                            }
//                        }else {
//                            throw new IllegalArgumentException("can not cast to LIST,key="+key+",value="+value.toString());
//                        }
//                        break;
//                    case DATETIME:
//                        if(value instanceof  String){
//                            try{
//                                newObj.put(column.getColumn_name(),Long.valueOf(datetimeStringToMsTimestamp(String.valueOf(value))));
//                            }catch (Exception e){
//                                logger.error("can not cast to DATETIME,key="+key+",value="+value.toString(),e);
//                                newObj.put(column.getColumn_name(),null);
//                            }
//
//                        }else {
//                            throw new IllegalArgumentException("can not cast to DATE,key="+key+",value="+value.toString());
//                        }
//                        break;
//                    case UNKNOWN:
//                }
//            }
//            newJsonList.add(newObj.toJSONString());
//        }
//        return newJsonList;
//    }

    public static  Object convert(String key,PropertyColumnEntry column, Object value){
        System.out.print("value type:" + value.getClass());
        switch (DataTypeSensorToCK.indexOf(column.getData_type())){
            case STRING:
                if(value instanceof String || value instanceof Integer || value instanceof Long || value instanceof Float || value instanceof Double ){
                    return String.valueOf(value);
                }else {
                    throw new IllegalArgumentException("can not cast to STRING,key="+key+",value="+value.toString());
                }
            case NUMBER:
                Double double_=null;
                if (value instanceof Long) {
                    double_ = Double.valueOf(((Long)value).longValue());
                } else if (value instanceof Integer) {
                    double_ = Double.valueOf(((Integer)value).intValue());
                } else if (value instanceof Double) {
                    double_ = (Double)value;
                } else if (value instanceof Float) {
                    double_ = Double.valueOf(((Float)value).floatValue());
                } else if (value instanceof String) {
                    double_ = Double.valueOf(String.valueOf(value));
                } else if(value instanceof BigDecimal) {
                    double_ = Double.valueOf(String.valueOf(value));
                } else {
                    throw new IllegalArgumentException("can not cast to NUMBER,key="+key+",value="+value.toString());
                }
                return BigDecimal.valueOf(double_).multiply(NUMBER_FACTOR_BIG_DECIMAL).longValue();
            case DATE:
                if(value instanceof  String){
                    try{
                        return Long.valueOf(dateStringToMsTimestamp(String.valueOf(value)));
                    }catch (Exception e){
                        logger.error("can not cast to DATE,key="+key+",value="+value.toString(),e);
                        return null;
                    }
                }else {
                    throw new IllegalArgumentException("can not cast to DATE,key="+key+",value="+value.toString());
                }
            case BOOL:
                Boolean bool=null;
                if (value instanceof Boolean) {
                    bool = ((Boolean)value).booleanValue();
                } else if (value instanceof Long) {
                    bool = (((Long)value).longValue() != 0L);
                } else if (value instanceof Integer) {
                    bool = (((Integer)value).intValue() != 0);
                } else {
                    throw new IllegalArgumentException("can not cast to BOOL,key="+key+",value="+value.toString());
                }
                return bool?1:0;
            case LIST:
                Collection<Object> values=new ArrayList();
                if(value instanceof String[]){
                    values.addAll(Arrays.asList((String[])value));
                }else if(Lists.class.isAssignableFrom(value.getClass())){
                    values.addAll((List)value);
                }else if(Set.class.isAssignableFrom(value.getClass())){
                    values.addAll((Set)value);
                }else {
                    logger.warn("can not cast to LIST,key="+key+",value="+value.toString());
                    //throw new IllegalArgumentException("can not cast to LIST,key="+key+",value="+value.toString());
                    return value.toString();
                }
                List<String> rstList=Lists.newArrayList();
                Set<String>  rstSet=Sets.newHashSet();
                for(Object o:values){
                    if(o instanceof String){
                        String str=normalize((String)o,false);
                        if(StringUtils.isNotBlank(str)){
                            rstList.add(str);
                            rstSet.add(str);
                        }
                    }
                }
                if(column.getTrue_list()==1){
                    return rstList.isEmpty()?null:Joiner.on(",").join(rstList);
                }else {
                    return rstSet.isEmpty()?null:Joiner.on(",").join(rstSet);
                }
            case DATETIME:
                if(value instanceof  String){
                    try{
                        return Long.valueOf(datetimeStringToMsTimestamp(String.valueOf(value)));
                    }catch (Exception e){
                        logger.error("can not cast to DATETIME,key="+key+",value="+value.toString(),e);
                        return null;
                    }

                }else {
                    throw new IllegalArgumentException("can not cast to DATE,key="+key+",value="+value.toString());
                }
            case UNKNOWN:
                return null;
        }
        return null;
    }







    private static final Set<String> eventType= Sets.newHashSet("track","track_signup");

    public static boolean isEventType(String type){
        return StringUtils.isNotEmpty(type) && eventType.contains(type);
    }


    private static final Pattern BAD_CHARS_PATTERN = Pattern.compile("\\s*");

    private static final Pattern BAD_CHARS_PATTERN_WITHOUT_SPACE = Pattern.compile("[\\f\\n\\r\\t\\v]*");


    public static String trimString(String paramString) {
        Matcher matcher = BAD_CHARS_PATTERN.matcher(paramString);
        return matcher.replaceAll("");
    }

      public static String trimStringWithoutSpace(String paramString) {
          Matcher matcher = BAD_CHARS_PATTERN_WITHOUT_SPACE.matcher(paramString);
          return matcher.replaceAll("");
      }

     public static String normalize(String paramString, boolean paramBoolean) {
         if (paramBoolean) {
             return trimStringWithoutSpace(paramString);
         }
         return trimString(paramString);
     }


    public static DataTypeSensorToCK judgeDataType(String paramString, Object paramObject, Set<String> paramSet) {
        if (paramObject instanceof String) {
            if (paramSet != null) {
                if (paramSet.contains(paramString)) {
                    return DataTypeSensorToCK.DATETIME;
                }
                return DataTypeSensorToCK.STRING;
            }
            return DataTypeSensorToCK.STRING;
        }
        if (paramObject instanceof Long || paramObject instanceof Integer || paramObject instanceof Float || paramObject instanceof Double){
            return DataTypeSensorToCK.NUMBER;
        }
        if (paramObject instanceof Boolean){
            return DataTypeSensorToCK.BOOL;
        }
        if (paramObject instanceof java.util.List || paramObject instanceof String[]) {
            return DataTypeSensorToCK.LIST;
        }
        return DataTypeSensorToCK.UNKNOWN;
    }


    public static final FastDateFormat[] LOADABLE_DATETIME_FORMATS = new FastDateFormat[] { FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS"),
            FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss"), FastDateFormat.getInstance("yyyy-MM-dd") };

    public static final FastDateFormat DATE_TIME_FORMAT=FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

    public static final FastDateFormat DATE_FORMAT=FastDateFormat.getInstance("yyyy-MM-dd");


    public static long dateStringToMsTimestamp(String paramString) throws ParseException {
        return FastDateFormat.getInstance("yyyy-MM-dd").parse(paramString).getTime();
    }

    public static String  dateTimeFormat(long date){
        return DATE_TIME_FORMAT.format(date);
    }

    public static String  dateFormat(long date){
        return DATE_FORMAT.format(date);
    }


    public static Date datetimeStringToDate(String paramString) throws ParseException {
        Date date = null;
        for (FastDateFormat fastDateFormat : LOADABLE_DATETIME_FORMATS) {
            try {
                date = fastDateFormat.parse(paramString);
                break;
            } catch (ParseException parseException) {}
        }
        if (date == null) {
            throw new ParseException(String.format("bad datetime format(tried all format), source=[%s]", new Object[] { paramString }), 0);
        }
        return date;
    }

    public static long datetimeStringToMsTimestamp(String paramString) throws ParseException { return datetimeStringToDate(paramString).getTime(); }


    public static int calcSamplingGroup(long paramLong) { return (int)((paramLong % 64L + 64L) % 64L); }




    public static Long generateKafkaOffset(long kafkaOffset) {
        //return Long.valueOf(kafkaOffset * 100L + paramKafkaData.getPartitionId()); }
        return 0L;
    }


    public static Long getOffset(Long trickId){
        if(trickId!=null){
            return Math.abs(trickId);
        }
        return generateKafkaOffset(0);
    }

    public static final LocalDateTime THE_EPOCH = (new DateTime(0L, DateTimeZone.UTC)).toLocalDateTime();

    private static final LocalDateTime WEEK_START_TIME=(new DateTime(-259200000L, DateTimeZone.UTC)).toLocalDateTime();

    private static final BigDecimal NUMBER_FACTOR_BIG_DECIMAL=BigDecimal.valueOf(1000);
    public static int calcMonthId(LocalDateTime paramLocalDateTime) {
        int i = Months.monthsBetween((ReadablePartial)THE_EPOCH, (ReadablePartial)paramLocalDateTime).getMonths();
        if (THE_EPOCH.isAfter((ReadablePartial)paramLocalDateTime)) {
            return i - 1;
        }
        return i;
    }

    public static int calcWeekId(LocalDateTime paramLocalDateTime) {
        int i = Weeks.weeksBetween((ReadablePartial)WEEK_START_TIME, (ReadablePartial)paramLocalDateTime).getWeeks();
        if (WEEK_START_TIME.isAfter((ReadablePartial)paramLocalDateTime)) {
            return i - 1;
        }
        return i;
    }


    public static int calcDayId(LocalDateTime paramLocalDateTime) {
        int i = Days.daysBetween((ReadablePartial)THE_EPOCH, (ReadablePartial)paramLocalDateTime).getDays();
        if (THE_EPOCH.isAfter((ReadablePartial)paramLocalDateTime)) {
            return i - 1;
        }
        return i;
    }

    public static int calcMonthId(long paramLong) { return calcMonthId((new DateTime(paramLong)).toLocalDateTime()); }

    public static int calcWeekId(long paramLong) { return calcWeekId((new DateTime(paramLong)).toLocalDateTime()); }


    public static void sort(List<String> list){
        list.sort(new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        });
    }


    public static Map<String,PropertyColumnEntry> getColumnMap(Map<String,PropertyColumnEntry> proMap){
        return proMap.values().stream().collect(Collectors.toMap(x->x.getColumn_name(),x->x));
    }


    public static void main(String [] args){

//        String x="{\"_track_id\":-1460706859,\"time\":1567069470371,\"type\":\"track\",\"distinct_id\":\"44pqswlAYKGMlKKxZmEe+vusWaK/cSy1x0Jb5/P6B7TQmZGCpQNeKEvPn7WPLmvT\",\"lib\":{\"$lib\":\"Android\",\"$lib_version\":\"3.2.3\",\"$app_version\":\"7.5.2\",\"$lib_method\":\"code\",\"$lib_detail\":\"com.creditwealth.client.ui.yiri.FutureGuideActivity######\"},\"event\":\"$AppClick\",\"properties\":{\"$device_id\":\"90521e3203711fbb\",\"$model\":\"OPPO A77\",\"$os_version\":\"7.1.1\",\"$app_version\":\"7.5.2\",\"$manufacturer\":\"OPPO\",\"$screen_height\":1920,\"$os\":\"Android\",\"$carrier\":\"中国移动\",\"$screen_width\":1080,\"$lib_version\":\"3.2.3\",\"$lib\":\"Android\",\"$wifi\":false,\"$network_type\":\"4G\",\"$element_id\":\"guide_icon_img\",\"$screen_name\":\"com.creditwealth.client.ui.yiri.FutureGuideActivity\",\"$title\":\"宜人财富\",\"$element_type\":\"android.widget.LinearLayout\",\"$is_first_day\":true,\"$ip\":\"223.104.64.170\",\"$is_login_id\":false,\"$city\":\"广州\",\"$province\":\"广东\",\"$country\":\"中国\"},\"_flush_time\":1567069485458,\"map_id\":\"90521e3203711fbb\",\"user_id\":6168488340386478321,\"recv_time\":1567069483528,\"extractor\":{\"f\":\"(dev=803,ino=808105909)\",\"o\":1983637,\"n\":\"access_log.2019082917\",\"s\":27786030,\"c\":27786030,\"e\":\"data01.yixin.sa\"},\"project_id\":1,\"project\":\"default\",\"ver\":2}";
//
//        EventEntry e=new EventEntry();
//        e.setName("$AppClick");
//        e.setBucket_id(12);
//        e.setId(0);
//        Map<String,EventEntry> eventMap= Maps.newHashMap();
//        eventMap.put(e.getName(),e);
//        JSONObject o= JSON.parseObject(x);
//        JSONObject p=o.getJSONObject("properties");
//        int c=0;
//        Map<String,PropertyEntry> proMap=Maps.newHashMap();
//        Map<Integer,PropertyColumnEntry> colMap=Maps.newHashMap();
//        for(String k:p.keySet()){
//            c++;
//            PropertyEntry y=new PropertyEntry();
//            y.setId(c);
//            y.setName(k);
//            y.setView_column_name("p__"+k);
//            y.setData_type(DataType.STRING.getIndex());
//            y.setIs_load(1);
//            proMap.put(k,y);
//            PropertyColumnEntry a=new PropertyColumnEntry();
//            a.setProperty_define_id(y.getId());
//            a.setColumn_name(y.getView_column_name());
//            a.setData_type(DataType.STRING.getIndex());
//            colMap.put(a.getProperty_define_id(),a);
//        }
//        List<String> s=covert(Lists.newArrayList(x),p.keySet(),eventMap,proMap,colMap);
//        System.out.println(s);






    }





}
