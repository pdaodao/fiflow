package com.github.lessonone.fiflow.common;

import java.util.Map;

public interface ConnectorBuilder {
    Map<String, String> build(String catalog, String databae, String tableName);
}