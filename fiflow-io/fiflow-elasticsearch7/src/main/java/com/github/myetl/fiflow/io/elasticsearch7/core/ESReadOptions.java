package com.github.myetl.fiflow.io.elasticsearch7.core;

/**
 * 读取数据相关的特定配置
 */
public class ESReadOptions {

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        public ESReadOptions build() {
            return new ESReadOptions();
        }
    }
}
