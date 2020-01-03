package com.github.myetl.flow.core.frame;

import com.github.myetl.flow.core.exception.SqlFlowConnectorBuildException;
import com.github.myetl.flow.core.exception.SqlFlowException;

import java.util.Map;

public interface ConnectorBuilder {


    /**
     * 构建 StreamTableSourceFactory 的 properties    即构建 ddl 的 with 部分
     *
     * @param tableConnectSetting 读写数据时表相关的配置
     * @return 返回 null 表示 不支持 flink 内部 TableFactory 方法生成 Source
     */
    Map<String, String> buildSourceProperties(TableConnectSetting tableConnectSetting)
            throws SqlFlowException;

    /**
     * 构建 StreamTableSinkFactory 的 properties  即构建 ddl 的 with 部分
     *
     * @param tableConnectSetting 读写数据时表相关的配置
     * @return 返回 null 表示 不支持 flink 内部 TableFactory 方法生成 Sink
     */
    Map<String, String> buildSinkProperties(TableConnectSetting tableConnectSetting)
            throws SqlFlowException;
}
