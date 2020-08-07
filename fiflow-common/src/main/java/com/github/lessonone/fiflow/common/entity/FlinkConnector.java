package com.github.lessonone.fiflow.common.entity;

import com.github.lessonone.fiflow.common.base.Table;
import lombok.Data;

import java.util.Map;


@Data
@Table("fi_flink_connector")
public class FlinkConnector extends BaseEntity {
    private Long typeId;
    private String typeName;

    // 连接属性
    private Map<String, FlinkConnectorType.OptionDescriptor> options;
    // 读属性
    private Map<String, FlinkConnectorType.OptionDescriptor> readOptions;
    // lookup 属性
    private Map<String, FlinkConnectorType.OptionDescriptor> lookupOptions;
    // 写属性
    private Map<String, FlinkConnectorType.OptionDescriptor> writeOptions;
}
