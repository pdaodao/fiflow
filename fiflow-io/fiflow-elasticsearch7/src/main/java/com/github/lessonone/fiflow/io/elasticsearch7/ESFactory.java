package com.github.lessonone.fiflow.io.elasticsearch7;

import com.github.lessonone.fiflow.io.elasticsearch7.core.ESOptions;
import com.github.lessonone.fiflow.io.elasticsearch7.sink.ESTableSink;
import com.github.lessonone.fiflow.io.elasticsearch7.source.ESTableSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.lessonone.fiflow.io.elasticsearch7.ESValidator.*;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.TABLE_SCHEMA_EXPR;
import static org.apache.flink.table.descriptors.Schema.*;

public class ESFactory implements
        StreamTableSourceFactory<Row>,
        StreamTableSinkFactory<Tuple2<Boolean, Row>> {

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_ELASTICSEARCH);
        context.put(CONNECTOR_PROPERTY_VERSION, "1");
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();

        // common options
        properties.add(CONNECTOR_HOSTS);
        properties.add(CONNECTOR_INDEX);

        properties.add(CONNECTOR_USERNAME);
        properties.add(CONNECTOR_PASSWORD);

        properties.add(CONNECTOR_PROPERTY_VERSION);

        // schema
        properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_NAME);
        // computed column
        properties.add(SCHEMA + ".#." + TABLE_SCHEMA_EXPR);

        return properties;
    }

    @Override
    public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
        DescriptorProperties descriptorProperties = getValidatedProperties(properties);
        TableSchema schema = TableSchemaUtils.getPhysicalSchema(
                descriptorProperties.getTableSchema(SCHEMA));

        return ESTableSource.builder()
                .setEsOptions(getESOptions(descriptorProperties))
                .setSchema(schema)
                .build();
    }

    @Override
    public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> properties) {
        DescriptorProperties descriptorProperties = getValidatedProperties(properties);
        TableSchema schema = TableSchemaUtils.getPhysicalSchema(
                descriptorProperties.getTableSchema(SCHEMA));

        return ESTableSink.builder()
                .setEsOptions(getESOptions(descriptorProperties))
                .setSchema(schema)
                .build();
    }

    private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);

        new SchemaValidator(true, false, false).validate(descriptorProperties);
        new ESValidator().validate(descriptorProperties);

        return descriptorProperties;
    }

    private ESOptions getESOptions(DescriptorProperties descriptorProperties) {
        return ESOptions.builder()
                .setHosts(descriptorProperties.getString(CONNECTOR_HOSTS))
                .setIndex(descriptorProperties.getString(CONNECTOR_INDEX))
                .setUsername(descriptorProperties.getOptionalString(CONNECTOR_USERNAME))
                .setPassword(descriptorProperties.getOptionalString(CONNECTOR_PASSWORD))
                .build();
    }

}
