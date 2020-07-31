package com.github.lessonone.fiflow.common.entity;

import lombok.Data;

import java.util.Map;

/**
 * fi_flink_connector
 */
@Data
public class FlinkConnector extends BaseEntity{
    private Long typeId;
    private String typeName;
    private Map<String, String> properties;
}
