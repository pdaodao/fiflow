package com.github.myetl.flow.core.frame;

import com.github.myetl.flow.core.ConnectorFactory;
import com.github.myetl.flow.core.connect.ConnectionProperties;
import com.github.myetl.flow.core.exception.SqlFlowConnectorBuildException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * 加载 ConnectorFactory
 */
public class ConnectorFactoryLoader {
    public static List<ConnectorFactory> factoryList = null;

    public static synchronized List<ConnectorFactory> load() {
        if(factoryList != null) return factoryList;

        ServiceLoader<ConnectorFactory> serviceLoader = ServiceLoader.load(ConnectorFactory.class);

        factoryList = IteratorUtils.toList(serviceLoader.iterator());

        return factoryList;
    }

    /**
     * 根据连接信息获取 ConnectorFactory
     * @param connectionProperties
     * @return
     * @throws SqlFlowConnectorBuildException
     */
    public static ConnectorFactory getConnectorFactory(ConnectionProperties connectionProperties)
        throws SqlFlowConnectorBuildException {

        if(StringUtils.isEmpty(connectionProperties.getConnectorType())){
            throw new SqlFlowConnectorBuildException("ConnectionProperties connectorType is empty");
        }

        List<ConnectorFactory> factories = load();
        if(CollectionUtils.isEmpty(factories))
            throw new SqlFlowConnectorBuildException("ConnectorFactory is empty");

        List<ConnectorFactory> result = new ArrayList<>();

        for(ConnectorFactory factory: factories){
            if(factory.canHandle(connectionProperties.getConnectorType())){
                result.add(factory);
            }
        }
        if(CollectionUtils.isEmpty(result))
            throw new SqlFlowConnectorBuildException("no ConnectorFactory can process ConnectorType:"+connectionProperties.getConnectorType());
        if(result.size() > 1)
            throw new SqlFlowConnectorBuildException("multiply ConnectorFactory can process "+connectionProperties.getConnectorType());
        return result.get(0);
    }


}
