package com.github.lessonone.fiflow.common.entity;

import com.github.lessonone.fiflow.common.base.Table;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
@Table("fi_flink_connector_type")
public class FlinkConnectorType extends BaseEntity {
    private Long id;
    private Long pid;
    private String name;
    private Boolean enabled;
    private Map<String, String> properties;
    private List<ParamDescriptor> descriptor;

    @Data
    public static class ParamDescriptor {
        private String name;
        private String key;
        private List<String> values;
        private boolean required = false;
    }

}
