package com.github.lessonone.fiflow.io.elasticsearch7;

import com.github.lessonone.fiflow.core.util.Preconditions;
import com.github.lessonone.fiflow.io.elasticsearch7.core.ESOptions;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.Map;

import static com.github.lessonone.fiflow.io.elasticsearch7.ESValidator.*;

@PublicEvolving
public class ES extends ConnectorDescriptor {
    private ESOptions esOptions;

    public ES() {
        super(CONNECTOR_TYPE_VALUE_ELASTICSEARCH, 1, false);
    }

    public ES setEsOptions(ESOptions esOptions) {
        this.esOptions = esOptions;
        return this;
    }

    @Override
    protected Map<String, String> toConnectorProperties() {
        Preconditions.checkNotNull(esOptions, "esOption should supplied.");

        DescriptorProperties properties = new DescriptorProperties();
        properties.putString(CONNECTOR_HOSTS, esOptions.getHosts());
        properties.putString(CONNECTOR_INDEX, esOptions.getIndex());

        if (esOptions.getUsername() != null && esOptions.getPassword() != null) {
            properties.putString(CONNECTOR_USERNAME, esOptions.getUsername());
            properties.putString(CONNECTOR_PASSWORD, esOptions.getPassword());
        }

        return properties.asMap();
    }
}
