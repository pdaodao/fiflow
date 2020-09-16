package com.github.lessonone.fiflow.common.entity;

import com.github.lessonone.fiflow.common.base.Table;
import com.github.lessonone.fiflow.common.pojo.ClusterType;
import lombok.Data;

@Data
@Table("fi_cluster")
public class ClusterEntity extends BaseEntity{
    // 编码
    private String name;
    // 描述
    private String comment;
    // 集群类型
    private ClusterType clusterType;
    // 连接地址
    private String host;
    // 连接端口
    private Integer port;
    // 插槽数
    private Integer slots;
    // 依赖的jar
    private String dependJars;
    // 是否删除
    private Boolean isDelete = false;
}
