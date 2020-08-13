package com.github.lessonone.fiflow.common.entity;

import com.github.lessonone.fiflow.common.base.Table;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashMap;
import java.util.Map;

@Data
@Table("fi_flink_connector_type")
public class FlinkConnectorType extends BaseEntity {
    private Long id;
    private String pname;
    private String name;
    private Boolean enabled;
    private String comment;

    // 实体 key 为了提取 数据表名称 主题名称 文件名称
    private String objectKey;

    // 连接属性
    private Map<String, OptionDescriptor> options;
    // 读属性
    private Map<String, OptionDescriptor> readOptions;
    // lookup 属性
    private Map<String, OptionDescriptor> lookupOptions;
    // 写属性
    private Map<String, OptionDescriptor> writeOptions;

    public static Map<String, OptionDescriptor> mergeDescriptor(Map<String, OptionDescriptor> opt, Map<String, OptionDescriptor> parent) {
        Map<String, OptionDescriptor> ret = new LinkedHashMap<>();
        if (parent != null) ret.putAll(parent);
        if (opt != null) ret.putAll(opt);
        return ret;
    }

    public String getConnector() {
        if (StringUtils.isNotEmpty(pname)) return pname;
        return name;
    }

    public FlinkConnectorType merge(FlinkConnectorType parent) {
        FlinkConnectorType merge = new FlinkConnectorType();
        merge.id = this.id;
        merge.pname = this.pname;
        merge.name = this.name;
        merge.enabled = this.enabled;
        merge.objectKey = this.objectKey;
        if (StringUtils.isEmpty(merge.objectKey))
            merge.objectKey = parent.getObjectKey();

        merge.options = mergeDescriptor(this.options, parent.getOptions());
        merge.readOptions = mergeDescriptor(this.readOptions, parent.getReadOptions());
        merge.lookupOptions = mergeDescriptor(this.lookupOptions, parent.getLookupOptions());
        merge.writeOptions = mergeDescriptor(this.writeOptions, parent.getWriteOptions());
        return merge;
    }

    @Data
    public static class OptionDescriptor {
        private String name;
        private String value;
        private String type;
        private boolean required = false;
        private String group;
    }
}
