package edp.wormhole.sparkx.swifts.custom.maidian;

import com.google.common.base.Joiner;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author daemon
 * @Date 19/7/19 15:20
 * To change this template use File | Settings | File Templates.
 */
public enum FilterFunction {
    EQUAL("equal") {
        @Override
        public String getFieldWhereClause(String field, DataType dataType, List<String> param) {
            if (CollectionUtils.isEmpty(param)) {
                return null;
            }
            StringBuilder where = new StringBuilder();
            switch (dataType) {
                case TEXT:
                case TIME:
                    List<String> ps = param.stream().filter(x -> StringUtils.isNotEmpty(x))
                            .map(x -> new StringBuilder().append("'").append(x).append("'").toString()).collect(Collectors.toList());
                    where.append(" ").append(field).append(" in ( ").append(Joiner.on(",").join(ps).toString()).append(" )");
                    break;
                case NUMBER:
                    where.append(" ").append(field).append(" in ( ").append(Joiner.on(",").join(param).toString()).append(" )");
                    break;
                case LOGIC:
                    throw new IllegalArgumentException("unsupported dataType=" + dataType + " on this function=" + this.name());
            }
            return where.toString();
        }
    },
    NOT_EQUAL("notEqual") {
        @Override
        public String getFieldWhereClause(String field, DataType dataType, List<String> param) {
            if (CollectionUtils.isEmpty(param)) {
                return null;
            }
            StringBuilder where = new StringBuilder();
            switch (dataType) {
                case TEXT:
                case TIME:
                    List<String> ps = param.stream().filter(x -> StringUtils.isNotEmpty(x))
                            .map(x -> new StringBuilder().append("'").append(x).append("'").toString()).collect(Collectors.toList());
                    where.append(" ").append(field).append(" not in ( ").append(Joiner.on(",").join(ps).toString()).append(" )");
                    break;
                case NUMBER:
                    where.append(" ").append(field).append(" not in ( ").append(Joiner.on(",").join(param).toString()).append(" )");
                    break;
                case LOGIC:
                    throw new IllegalArgumentException("unsupported dataType=" + dataType + " on this function=" + this.name());
            }
            return where.toString();
        }
    },
    CONTAIN("contain") {
        @Override
        public String getFieldWhereClause(String field, DataType dataType, List<String> param) {
            if (CollectionUtils.isEmpty(param)) {
                return null;
            }
            StringBuilder where = new StringBuilder();
            switch (dataType) {
                case TEXT:
                    where.append(" ").append(field).append(" in ( ").append("'").append(param.get(0)).append("'").append(" )");
                    break;
                case NUMBER:
                    where.append(" ").append(field).append(" in ( ").append(param.get(0)).append(" )");
                    break;
                case TIME:
                case LOGIC:
                    throw new IllegalArgumentException("unsupported dataType=" + dataType + " on this function=" + this.name());
            }
            return where.toString();
        }
    },
    NOT_CONTAIN("notContain") {
        @Override
        public String getFieldWhereClause(String field, DataType dataType, List<String> param) {
            if (CollectionUtils.isEmpty(param)) {
                return null;
            }
            StringBuilder where = new StringBuilder();
            switch (dataType) {
                case TEXT:
                    where.append(" ").append(field).append(" not in ( ").append("'").append(param.get(0)).append("'").append(" )");
                    break;
                case NUMBER:
                    where.append(" ").append(field).append(" not in ( ").append(param.get(0)).append(" )");
                    break;
                case TIME:
                case LOGIC:
                    throw new IllegalArgumentException("unsupported dataType=" + dataType + " on this function=" + this.name());
            }
            return where.toString();
        }
    },
    IS_SET("isSet") {
        @Override
        public String getFieldWhereClause(String field, DataType dataType, List<String> param) {
            StringBuilder where = new StringBuilder();
            switch (dataType) {
                case TEXT:
                case NUMBER:
                case TIME:
                case LOGIC:
                    where.append(" isNotNull(").append(field).append(")");
                    break;
            }
            return where.toString();
        }
    },
    NOT_SET("notSet") {
        @Override
        public String getFieldWhereClause(String field, DataType dataType, List<String> param) {
            StringBuilder where = new StringBuilder();
            switch (dataType) {
                case TEXT:
                case NUMBER:
                case TIME:
                case LOGIC:
                    where.append(" isNull(").append(field).append(")");
                    break;
            }
            return where.toString();
        }
    },
    EMPTY("empty") {
        @Override
        public String getFieldWhereClause(String field, DataType dataType, List<String> param) {
            StringBuilder where = new StringBuilder();
            switch (dataType) {
                case TEXT:
                    //where.append(" (").append(field).append("='')");
                    where.append(field).append("=''");
                    break;
                case NUMBER:
                case TIME:
                case LOGIC:
                    where.append(NOT_SET.getFieldWhereClause(field, dataType, param));
                    break;
            }
            return where.toString();
        }
    },
    NOT_EMPTY("notEmpty") {
        @Override
        public String getFieldWhereClause(String field, DataType dataType, List<String> param) {
            StringBuilder where = new StringBuilder();
            switch (dataType) {
                case TEXT:
                    //where.append(" (").append(field).append("!='')");
                    where.append(field).append("!=''");
                    break;
                case NUMBER:
                case TIME:
                case LOGIC:
                    where.append(IS_SET.getFieldWhereClause(field, dataType, param));
                    break;
            }
            return where.toString();
        }
    },
    /*LIKE("like") {
        @Override
        public String getFieldWhereClause(String field, DataType dataType, List<String> param) {
            if (CollectionUtils.isEmpty(param)) {
                return null;
            }
            StringBuilder where = new StringBuilder();
            switch (dataType) {
                case TEXT:
//                    where.append(" match(").append(field).append(field).append(",").append("'").append("\\(.*)")
//                            .append("(").append(param.get(0)).append(")").append("(.*)").append(")=1");
                    where.append(" match(").append(field).append(",").append("'").append(param.get(0)).append("')=1");
                    break;
                case NUMBER:
                case TIME:
                case LOGIC:
                    throw new IllegalArgumentException("unsupported dataType=" + dataType + " on this function=" + this.name());
            }
            return where.toString();
        }
    },
    NOT_LIKE("notLike") {
        @Override
        public String getFieldWhereClause(String field, DataType dataType, List<String> param) {
            if (CollectionUtils.isEmpty(param)) {
                return null;
            }
            StringBuilder where = new StringBuilder();
            switch (dataType) {
                case TEXT:
//                    where.append(" match(").append(field).append(field).append(",").append("'").append("\\(.*)")
//                            .append("(").append(param.get(0)).append(")").append("(.*)").append(")=0");
                    where.append(" match(").append(field).append(",").append("'").append(param.get(0)).append("')=0");
                    break;
                case NUMBER:
                case TIME:
                case LOGIC:
                    throw new IllegalArgumentException("unsupported dataType=" + dataType + " on this function=" + this.name());
            }
            return where.toString();
        }
    },
    LEFT_LIKE("startWith") {
        @Override
        public String getFieldWhereClause(String field, DataType dataType, List<String> param) {
            if (CollectionUtils.isEmpty(param)) {
                return null;
            }
            StringBuilder where = new StringBuilder();
            switch (dataType) {
                case TEXT:
                    String reg = "\\^" + param.get(0) + ".*";
                    where.append(" match(").append(field).append(",").append("'").append(reg).append("')=1");
                    break;
                case NUMBER:
                case TIME:
                case LOGIC:
                    throw new IllegalArgumentException("unsupported dataType=" + dataType + " on this function=" + this.name());
            }
            return where.toString();
        }
    },
    RIGHT_LIKE("endWith") {
        @Override
        public String getFieldWhereClause(String field, DataType dataType, List<String> param) {
            if (CollectionUtils.isEmpty(param)) {
                return null;
            }
            StringBuilder where = new StringBuilder();
            switch (dataType) {
                case TEXT:
                    String reg = ".*" + param.get(0) + "\\$";
                    where.append(" match(").append(field).append(",").append("'").append(reg).append("')=1");
                    break;
                case NUMBER:
                case TIME:
                case LOGIC:
                    throw new IllegalArgumentException("unsupported dataType=" + dataType + " on this function=" + this.name());
            }
            return where.toString();
        }
    },
    INCLUDE_LIKE("include") {
        @Override
        public String getFieldWhereClause(String field, DataType dataType, List<String> param) {
            if (CollectionUtils.isEmpty(param)) {
                return null;
            }
            StringBuilder where = new StringBuilder();
            switch (dataType) {
                case TEXT:
                    String reg = ".*" + param.get(0) + ".*";
                    where.append(" match(").append(field).append(",").append("'").append(reg).append("')=1");
                    break;
                case NUMBER:
                case TIME:
                case LOGIC:
                    throw new IllegalArgumentException("unsupported dataType=" + dataType + " on this function=" + this.name());
            }
            return where.toString();
        }
    },*/
    LESS("less") {
        @Override
        public String getFieldWhereClause(String field, DataType dataType, List<String> param) {
            if (CollectionUtils.isEmpty(param)) {
                return null;
            }
            StringBuilder where = new StringBuilder();
            switch (dataType) {
                case NUMBER:
                case TIME:
                    where.append(" ").append(field).append("<").append(param.get(0));
                    break;
                case TEXT:
                case LOGIC:
                    throw new IllegalArgumentException("unsupported dataType=" + dataType + " on this function=" + this.name());
            }
            return where.toString();
        }
    },
    LESSTHANEQUALTO("lteq") {
        @Override
        public String getFieldWhereClause(String field, DataType dataType, List<String> param) {
            if (CollectionUtils.isEmpty(param)) {
                return null;
            }
            StringBuilder where = new StringBuilder();
            switch (dataType) {
                case NUMBER:
                case TIME:
                    where.append(" ").append(field).append("<=").append(param.get(0));
                    break;
                case TEXT:
                case LOGIC:
                    throw new IllegalArgumentException("unsupported dataType=" + dataType + " on this function=" + this.name());
            }
            return where.toString();
        }
    },
    GREATER("greater") {
        @Override
        public String getFieldWhereClause(String field, DataType dataType, List<String> param) {
            if (CollectionUtils.isEmpty(param)) {
                return null;
            }
            StringBuilder where = new StringBuilder();
            switch (dataType) {
                case NUMBER:
                case TIME:
                    where.append(" ").append(field).append(">").append(param.get(0));
                    break;
                case TEXT:
                case LOGIC:
                    throw new IllegalArgumentException("unsupported dataType=" + dataType + " on this function=" + this.name());
            }
            return where.toString();
        }
    },
    GREATERTHANEQUALTO("gteq") {
        @Override
        public String getFieldWhereClause(String field, DataType dataType, List<String> param) {
            if (CollectionUtils.isEmpty(param)) {
                return null;
            }
            StringBuilder where = new StringBuilder();
            switch (dataType) {
                case NUMBER:
                case TIME:
                    where.append(" ").append(field).append(">=").append(param.get(0));
                    break;
                case TEXT:
                case LOGIC:
                    throw new IllegalArgumentException("unsupported dataType=" + dataType + " on this function=" + this.name());
            }
            return where.toString();
        }
    },
    BETWEEN("between") {
        @Override
        public String getFieldWhereClause(String field, DataType dataType, List<String> param) {
            if (CollectionUtils.isEmpty(param)) {
                return null;
            }
            StringBuilder where = new StringBuilder();
            switch (dataType) {
                case NUMBER:
                case TIME:
                    where.append(" (").append(field).append(">='").append(param.get(0)).append(":00' and").append(" ")
                            .append(field).append("<='").append(param.get(1)).append(":00') ");
                    break;
                case TEXT:
                case LOGIC:
                    throw new IllegalArgumentException("unsupported dataType=" + dataType + " on this function=" + this.name());
            }
            return where.toString();
        }
    },
    TRUE("true") {
        @Override
        public String getFieldWhereClause(String field, DataType dataType, List<String> param) {
            StringBuilder where = new StringBuilder();
            switch (dataType) {
                case NUMBER:
                case TEXT:
                case TIME:
                    throw new IllegalArgumentException("unsupported dataType=" + dataType + " on this function=" + this.name());
                case LOGIC:
                    where.append(" ").append(field).append("=").append("1");
            }
            return where.toString();
        }
    },
    FALSE("false") {
        @Override
        public String getFieldWhereClause(String field, DataType dataType, List<String> param) {
            StringBuilder where = new StringBuilder();
            switch (dataType) {
                case NUMBER:
                case TEXT:
                case TIME:
                    throw new IllegalArgumentException("unsupported dataType=" + dataType + " on this function=" + this.name());
                case LOGIC:
                    where.append(" ").append(field).append("=").append("0");
            }
            return where.toString();
        }
    },;


    abstract public String getFieldWhereClause(String field, DataType dataType, List<String> param);


    private String function;


    private FilterFunction(String function) {
        this.function = function;
    }


    public String getFunction() {
        return function;
    }

    public static FilterFunction getFilterFunction(String function) {
        for (FilterFunction x : FilterFunction.values()) {
            if (x.function.equals(function)) {
                return x;
            }
        }
        return null;
    }
}