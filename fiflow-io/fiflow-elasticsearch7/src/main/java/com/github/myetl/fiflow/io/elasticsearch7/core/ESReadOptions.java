package com.github.myetl.fiflow.io.elasticsearch7.core;

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
