package com.github.lessonone.fiflow.common.entity;

import com.github.lessonone.fiflow.common.base.Table;
import lombok.Data;

import java.util.Map;


@Data
@Table("fi_flink_connector")
public class FlinkConnector extends BaseEntity {
    private Long typeId;
    private String typeName;
    private Map<String, String> properties;
}
