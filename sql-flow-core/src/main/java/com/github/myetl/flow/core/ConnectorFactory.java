package com.github.myetl.flow.core;

import com.github.myetl.flow.core.connect.ConnectionProperties;
import com.github.myetl.flow.core.frame.ConnectorBuilder;
import com.github.myetl.flow.core.frame.TableMetaReader;

/**
 * 不同类型的 连接器的 元信息 和 连接的 服务
 */
public interface ConnectorFactory {

    /**
     * 是否可以处理该类型的 连接器
     *
     * @param connectorType 连接器类型 jdbc kafka hbase ...
     * @return
     */
    boolean canHandle(String connectorType);

    /**
     * 数据表信息读取器
     *
     * @param connectionProperties 连接信息
     * @return 元信息读取器 可以为 null
     */
    TableMetaReader metaReader(ConnectionProperties connectionProperties);

    /**
     * 输入 输出 构造器
     *
     * @return 连接构建器 不可以为 null
     */
    ConnectorBuilder connectorBuilder(ConnectionProperties connectionProperties);
}
