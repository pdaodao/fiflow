package com.github.lessonone.fiflow.common.entity;

import lombok.Data;

import java.util.List;

/**
 * fi_flink_connector_type
 */
@Data
public class FlinkConnectorType extends BaseEntity{
    private Long id;
    private Long pid;
    private String name;
    private Boolean enabled;

    public static class ParamDescriptor{
        private String name;
        private String key;
        private List<String> values;
        private boolean required = false;
    }

}
