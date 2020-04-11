package com.github.myetl.fiflow.core.util;


import com.github.myetl.fiflow.core.sql.TableResultSink;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.experimental.CollectSink;
import org.apache.flink.streaming.experimental.SocketStreamIterator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.net.InetAddress;

public class FlinkUtils {


    public static void collect(Table table) throws Exception {
        TableEnvironment tEnv = ((TableImpl) table).getTableEnvironment();
        TableSchema schema = table.getSchema().copy();
        DataType rowDataType = schema.toRowDataType();

        @SuppressWarnings("unchecked")
        TypeSerializer<Row> serializer = (TypeSerializer<Row>) TypeInfoDataTypeConverter
                .fromDataTypeToTypeInfo(rowDataType)
                .createSerializer(new ExecutionConfig());

        InetAddress address = InetAddress.getByName("10.10.70.118");
        int port = 9092;

        System.out.println("-- address "+address.getHostAddress()+":"+port);

        final SocketStreamIterator<Row> iterator = new SocketStreamIterator<>(port, address, serializer);

        CollectSink<Row> collectSink = new CollectSink<>(address, port, serializer);

        TableResultSink sink = new TableResultSink(schema, collectSink);
        String tableName = "haha";

        ((TableImpl) table).getTableEnvironment().registerTableSink(tableName, sink);

        table.insertInto(tableName);
        receiveData(iterator);
    }

    public static void receiveData(final SocketStreamIterator<Row> iterator){
        Thread getMessageThread = new Thread(){
            public volatile boolean isRunning = true;
            @Override
            public void run() {
                try{
                    while(isRunning && iterator.hasNext()){
                        Row row = iterator.next();
                        System.out.print("---- ");
                        System.out.println(row.toString());
                    }
                }catch (Exception e){
                    isRunning = false;
                }
            }
        };
        getMessageThread.start();
    }



    // TableUtils.collectToList
//    /**
//     * Convert Flink table to Java list.
//     * This method is only applicable for small batch jobs and small finite append only stream jobs.
//     *
//     * @param table		Flink table to convert
//     * @return			Converted Java list
//     */
//    public static void collectToList(Table table) throws Exception {
//        TableEnvironment tEnv = ((TableImpl) table).getTableEnvironment();
//        String id = new AbstractID().toString();
//
//        TableSchema schema = table.getSchema().copy();
//        DataType rowDataType = schema.toRowDataType();
//
//        @SuppressWarnings("unchecked")
//        TypeSerializer<Row> serializer = (TypeSerializer<Row>) TypeInfoDataTypeConverter
//                .fromDataTypeToTypeInfo(rowDataType)
//                .createSerializer(new ExecutionConfig());
//
//        Utils.CollectHelper<Row> outputFormat = new Utils.CollectHelper<>(id, serializer);
//
//        TableUtils.TableResultSink sink = new TableUtils.TableResultSink(schema, outputFormat);
//
//        String tableName = table.toString();
//        String sinkName = "tableResultSink_" + tableName + "_" + id;
//        String jobName = "tableResultToList_" + tableName + "_" + id;
//
//        List<Row> deserializedList;
//        try {
//            tEnv.registerTableSink(sinkName, sink);
//            tEnv.insertInto(sinkName, table);
//            JobExecutionResult executionResult = tEnv.execute(jobName);
//            ArrayList<byte[]> accResult = executionResult.getAccumulatorResult(id);
//            deserializedList = SerializedListAccumulator.deserializeList(accResult, serializer);
//        } finally {
//            tEnv.dropTemporaryTable(sinkName);
//        }
//        return deserializedList;
//    }

    
}
