package com.github.myetl.fiflow.core.sql;

import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;
import java.util.regex.Pattern;

/**
 * 把 sql / 命令 转为 flink 中的操作
 */
public interface CmdBuilder {

    int DEFAULT_PATTERN_FLAGS = Pattern.CASE_INSENSITIVE | Pattern.DOTALL;

    /**
     * 该 builder 是否可以处理该 sql
     *
     * @param sql 单条 sql
     * @return
     */
    Optional<String[]> accept(String sql);

    /**
     * 构建
     *
     * @param cmd
     * @param session
     * @return
     */
    CmdBuildInfo build(Cmd cmd, FiflowSqlSession session);

    /**
     * 帮助信息
     * @return  使用;分隔为两部分
     */
    default String help(){
        String name = this.getClass().getSimpleName()
                .replace("Builder", "")
                .toLowerCase();


        return name + "; todo";
    }
}
