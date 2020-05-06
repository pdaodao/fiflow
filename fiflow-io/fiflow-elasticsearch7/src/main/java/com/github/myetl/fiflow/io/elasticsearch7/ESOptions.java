package com.github.myetl.fiflow.io.elasticsearch7;

import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class ESOptions {
    // 127.0.0.1:9200,127.0.0.2:9200
    private String hosts;
    // index name
    private String index;

    private String username;
    private String password;

    public ESOptions(String hosts, String index, String username, String password) {
        this.hosts = hosts;
        this.index = index;
        this.username = username;
        this.password = password;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getHosts() {
        return hosts;
    }

    public String getIndex() {
        return index;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public static class Builder {
        private String hosts;
        private String index;
        private String username;
        private String password;

        public Builder setHosts(String hosts) {
            this.hosts = hosts;
            return this;
        }

        public Builder setIndex(String index) {
            this.index = index;
            return this;
        }

        public Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder setUsername(Optional<String> username) {
            if (username.isPresent()) {
                this.username = username.get();
            }
            return this;
        }

        public Builder setPassword(Optional<String> password) {
            if (password.isPresent()) {
                this.password = password.get();
            }
            return this;
        }

        public ESOptions build() {
            checkNotNull(hosts, "No hosts supplied.");
            checkNotNull(index, "No indexName supplied.");
            return new ESOptions(hosts, index, username, password);
        }
    }


}
