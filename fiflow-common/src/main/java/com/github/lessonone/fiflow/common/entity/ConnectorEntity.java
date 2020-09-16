package com.github.lessonone.fiflow.common.entity;

import com.github.lessonone.fiflow.common.base.Table;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;


/**
 * 物理连接
 */
@Data
@Table("fi_connector")
public class ConnectorEntity extends BaseEntity {
    private String name;
    private String typeName;
    private String hashCode;
    private String comment;

    // 连接属性
    private Map<String, String> options;
    // 读属性
    private Map<String, String> readOptions;
    // lookup 属性
    private Map<String, String> lookupOptions;
    // 写属性
    private Map<String, String> writeOptions;

    public static void putMapIfAbsent(Map<String, String> target, Map<String, String> from) {
        if (from == null) return;
        for (Map.Entry<String, String> entry : from.entrySet()) {
            if (entry.getValue() != null) {
                target.putIfAbsent(entry.getKey(), entry.getValue());
            }
        }
    }

    public Map<String, String> mergeTableProperties(Map<String, String> tableProps) {
        Map<String, String> props = new HashMap<>();
        putMapIfAbsent(props, tableProps);
        putMapIfAbsent(props, options);
        putMapIfAbsent(props, readOptions);
        putMapIfAbsent(props, lookupOptions);
        putMapIfAbsent(props, writeOptions);
        return props;
    }
}
