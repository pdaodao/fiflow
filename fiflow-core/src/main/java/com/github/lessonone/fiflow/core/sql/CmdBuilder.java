package com.github.lessonone.fiflow.core.sql;

import com.github.lessonone.fiflow.core.flink.BuildLevel;
import com.github.lessonone.fiflow.core.flink.FlinkBuildInfo;

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
     * 构建级别
     *
     * @return
     */
    BuildLevel buildLevel();


    /**
     * 先预处理一下
     *
     * @param cmd
     * @param sqlBuilder
     */
    default void preBuild(Cmd cmd, SqlBuilder sqlBuilder) {

    }

    /**
     * 构建 把 sql 转换为 flink 中的操作
     *
     * @param cmd
     * @param sessionContext
     * @return
     */
    FlinkBuildInfo build(Cmd cmd, SqlSessionContext sessionContext);

    /**
     * 帮助信息
     *
     * @return 使用;分隔为两部分
     */
    default String help() {
        String name = this.getClass().getSimpleName()
                .replace("Builder", "")
                .toLowerCase();


        return name + "; todo";
    }
}
