package com.github.myetl.flow.core.frame;

/**
 * source sink 数据表时的 数据表的相关配置
 */
public class TableConnectSetting {
    /**
     * 数据表名称
     */
    public final String tableName;

    public TableConnectSetting(String tableName) {
        this.tableName = tableName;
    }
}
