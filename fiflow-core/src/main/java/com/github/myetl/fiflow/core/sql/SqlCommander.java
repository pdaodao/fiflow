package com.github.myetl.fiflow.core.sql;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * sql 命令 和 参数
 */
public final class SqlCommander {

    // 不需要参数
    private static final Function<String[], Optional<String[]>> NO_ARGS =
            (args) -> Optional.of(new String[0]);
    // 一个参数
    private static final Function<String[], Optional<String[]>> SINGLE_ARGS =
            (args) -> Optional.of(new String[]{args[0]});
    // 两个参数
    private static final Function<String[], Optional<String[]>> TWO_ARGS = new Function<String[], Optional<String[]>>() {
        @Override
        public Optional<String[]> apply(String[] args) {
            if (args.length < 2) {
                return Optional.empty();
            }
            return Optional.of(new String[]{args[0], args[1]});
        }
    };
    // 忽略大小写
    private static final int DEFAULT_PATTERN_FLAGS = Pattern.CASE_INSENSITIVE | Pattern.DOTALL;
    public final SqlCommand command;
    public final String[] args;

    public SqlCommander(SqlCommand sqlCommand, String[] args) {
        this.command = sqlCommand;
        this.args = args;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        SqlCommander that = (SqlCommander) o;

        return new EqualsBuilder()
                .append(command, that.command)
                .append(args, that.args)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(command)
                .append(args)
                .toHashCode();
    }

    @Override
    public String toString() {
        return command + "(" + Arrays.toString(args) + ")";
    }


    public enum SqlCommand {

        QUIT("(QUIT|EXIT)", NO_ARGS),

        CLEAR("CLEAR", NO_ARGS),

        HELP("HELP", NO_ARGS),

        SHOW_CATALOGS("SHOW\\s+CATALOGS", NO_ARGS),

        SHOW_DATABASES("SHOW\\s+DATABASES", NO_ARGS),

        SHOW_TABLES("SHOW\\s+TABLES", NO_ARGS),

        SHOW_FUNCTIONS("SHOW\\s+FUNCTIONS", NO_ARGS),

        SHOW_MODULES("SHOW\\s+MODULES", NO_ARGS),

        USE_CATALOG("USE\\s+CATALOG\\s+(.*)", SINGLE_ARGS),

        USE("USE\\s+(?!CATALOG)(.*)", SINGLE_ARGS),

        DESCRIBE("DESCRIBE\\s+(.*)", SINGLE_ARGS),

        EXPLAIN("EXPLAIN\\s+(.*)", SINGLE_ARGS),

        SELECT("(SELECT.*)", SINGLE_ARGS),

        INSERT_INTO("(INSERT\\s+INTO.*)", SINGLE_ARGS),

        INSERT_OVERWRITE("(INSERT\\s+OVERWRITE.*)", SINGLE_ARGS),

        CREATE_TABLE("(CREATE\\s+TABLE\\s+.*)", SINGLE_ARGS),

        DROP_TABLE("(DROP\\s+TABLE\\s+.*)", SINGLE_ARGS),

        CREATE_VIEW("CREATE\\s+VIEW\\s+(\\S+)\\s+AS\\s+(.*)", TWO_ARGS),

        CREATE_DATABASE("(CREATE\\s+DATABASE\\s+.*)", SINGLE_ARGS),

        DROP_DATABASE("(DROP\\s+DATABASE\\s+.*)", SINGLE_ARGS),

        DROP_VIEW("DROP\\s+VIEW\\s+(.*)", SINGLE_ARGS),

        ALTER_DATABASE("(ALTER\\s+DATABASE\\s+.*)", SINGLE_ARGS),

        ALTER_TABLE("(ALTER\\s+TABLE\\s+.*)", SINGLE_ARGS),

        SET("SET(\\s+(\\S+)\\s*=(.*))?", // whitespace is only ignored on the left side of '='
                (operands) -> {
                    if (operands.length < 3) {
                        return Optional.empty();
                    } else if (operands[0] == null) {
                        return Optional.of(new String[0]);
                    }
                    return Optional.of(new String[]{operands[1], operands[2]});
                }),

        RESET("RESET", NO_ARGS),

        SOURCE("SOURCE\\s+(.*)", SINGLE_ARGS),

        // jar xxx    需要添加的jar包
        JAR("-*\\s?jar\\s+(.*)", SINGLE_ARGS),

        // udf xxx    用户自定义 先这样写
        UDF("-*\\s?udf\\s+(.*)", SINGLE_ARGS),

        // flink xxx  表示使用那个 flink 集群
        FLINK("-*\\s?flink\\s+(.*)", SINGLE_ARGS),

        //  -p 3      并发
        PARALLELISM("-*\\s?-p\\s+(\\d)", SINGLE_ARGS);

        public final Pattern pattern;
        public final Function<String[], Optional<String[]>> argExtractFun;


        SqlCommand(String pattern, Function<String[], Optional<String[]>> argExtractFun) {
            this.pattern = Pattern.compile(pattern, DEFAULT_PATTERN_FLAGS);
            this.argExtractFun = argExtractFun;
        }

        @Override
        public String toString() {
            return super.toString().replace('_', ' ');
        }

        public boolean needArg() {
            return this.argExtractFun != NO_ARGS;
        }
    }

}



