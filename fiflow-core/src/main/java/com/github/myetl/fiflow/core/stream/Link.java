package com.github.myetl.fiflow.core.stream;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;

/**
 * 节点之间的连线
 */
public class Link implements Serializable {
    // 源节点id
    private String source;
    // 目标节点id
    private String target;
    // todo 属性


    public String getSource() {
        return source;
    }

    public Link setSource(String source) {
        this.source = source;
        return this;
    }

    public String getTarget() {
        return target;
    }

    public Link setTarget(String target) {
        this.target = target;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        Link connect = (Link) o;

        return new EqualsBuilder()
                .append(source, connect.source)
                .append(target, connect.target)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(source)
                .append(target)
                .toHashCode();
    }
}
