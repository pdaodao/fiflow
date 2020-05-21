package com.github.lessonone.fiflow.io.elasticsearch7;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

@Internal
public class ESValidator extends ConnectorDescriptorValidator {
    public static final String CONNECTOR_TYPE_VALUE_ELASTICSEARCH = "elasticsearch";
    public static final String CONNECTOR_HOSTS = "connector.hosts";
    public static final String CONNECTOR_INDEX = "connector.index";
    public static final String CONNECTOR_USERNAME = "connector.username";
    public static final String CONNECTOR_PASSWORD = "connector.password";

    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);
        properties.validateString(CONNECTOR_HOSTS, false, 1);
        properties.validateString(CONNECTOR_INDEX, false, 1);
        properties.validateString(CONNECTOR_USERNAME, true);
        properties.validateString(CONNECTOR_PASSWORD, true);
    }

}