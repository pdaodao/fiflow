package com.github.myetl.flow.core.frame;

import com.github.myetl.flow.core.ConnectorFactory;
import com.github.myetl.flow.core.connect.ConnectionProperties;
import com.github.myetl.flow.core.exception.SqlFlowConnectorBuildException;
import java.util.HashMap;
import java.util.Map;

public class FactoryManager {
    private static Map<Integer, TableMetaReader> metaReaderMap = new HashMap<>();

    public static TableMetaReader metaReader(ConnectionProperties connectionProperties) throws SqlFlowConnectorBuildException {
        Integer id = connectionProperties.id;
        if(metaReaderMap.containsKey(id)){
            return metaReaderMap.get(id);
        }
        ConnectorFactory connectorFactory = ConnectorFactoryLoader.getConnectorFactory(connectionProperties);

        TableMetaReader metaReader = connectorFactory.metaReader(connectionProperties);
        metaReaderMap.put(id, metaReader);
        return metaReader;
    }

    public static ConnectorBuilder connectorBuilder(ConnectionProperties connectionProperties) throws SqlFlowConnectorBuildException {
        ConnectorFactory connectorFactory = ConnectorFactoryLoader.getConnectorFactory(connectionProperties);
        return connectorFactory.connectorBuilder(connectionProperties);
    }

    public static void closeAll() {
        metaReaderMap.values().forEach(metaReader -> {
            try{
                metaReader.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        });
        metaReaderMap = new HashMap<>();
    }

//    public void close(ConnectionProperties connectionProperties){
//        Integer id = connectionProperties.id;
//        TableMetaReader metaReader = metaReaderMap.get(id);
//        if(metaReader != null ){
//            metaReader.close();
//            metaReaderMap.remove(id);
//        }
//    }

}
