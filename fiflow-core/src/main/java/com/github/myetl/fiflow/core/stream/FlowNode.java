package com.github.myetl.fiflow.core.stream;

import java.io.Serializable;

/**
 * flow 中的一个节点
 */
public class FlowNode implements Serializable {
    // 节点 id
    private String id;
    // 节点类型
    private NodeType type;


}
