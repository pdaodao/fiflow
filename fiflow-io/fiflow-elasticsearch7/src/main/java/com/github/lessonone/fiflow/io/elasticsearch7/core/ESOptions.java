package com.github.lessonone.fiflow.io.elasticsearch7.core;

import java.io.Serializable;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * elasticsearch 连接信息
 */
public class ESOptions implements Serializable {
    // 127.0.0.1:9200,127.0.0.2:9200
    private String hosts;
    // index name
    private String index;

    private String username;
    private String password;

    public static Builder builder() {
        return new Builder();
    }

    public String getHosts() {
        return hosts;
    }

    public ESOptions setHosts(String hosts) {
        this.hosts = hosts;
        return this;
    }

    public String getIndex() {
        return index;
    }

    public ESOptions setIndex(String index) {
        this.index = index;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public ESOptions setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public ESOptions setPassword(String password) {
        this.password = password;
        return this;
    }

    public static class Builder {
        private ESOptions options;

        public Builder() {
            options = new ESOptions();
        }

        public Builder setHosts(String hosts) {
            options.hosts = hosts;
            return this;
        }

        public Builder setIndex(String index) {
            options.index = index;
            return this;
        }

        public Builder setUsername(String username) {
            options.username = username;
            return this;
        }

        public Builder setUsername(Optional<String> username) {
            if (username.isPresent()) {
                options.username = username.get();
            }
            return this;
        }

        public Builder setPassword(Optional<String> password) {
            if (password.isPresent()) {
                options.password = password.get();
            }
            return this;
        }

        public ESOptions build() {
            checkNotNull(options.hosts, "No hosts supplied.");
            checkNotNull(options.index, "No indexName supplied.");
            return options;
        }
    }


}
