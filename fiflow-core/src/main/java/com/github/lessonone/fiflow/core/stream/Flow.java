package com.github.lessonone.fiflow.core.stream;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 使用节点 连线方式的完整的任务描述
 */
public class Flow implements Serializable {
    private List<FlowNode> nodes = new ArrayList<>();
    private List<Link> links = new ArrayList<>();


}
