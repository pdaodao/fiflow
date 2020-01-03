package com.github.myetl.flow.core.frame;

import com.github.myetl.flow.core.exception.SqlFlowMetaReaderException;
import com.github.myetl.flow.core.meta.DBTable;
import com.github.myetl.flow.core.meta.DBTableField;

import java.util.List;

/**
 * 数据表 元信息读取器
 */
public interface TableMetaReader {

    /**
     * 数据库下的数据表列表
     *
     * @return
     */
    List<DBTable> list() throws SqlFlowMetaReaderException;

    /**
     * 数据表的字段信息
     *
     * @param tableName 数据表名称
     * @return
     */
    List<DBTableField> fields(String tableName) throws SqlFlowMetaReaderException;

    /**
     * 关闭读取器 在这里会关闭数据库连接等
     * 在任务提交前 一定要调用该方法
     */
    void close();
}
