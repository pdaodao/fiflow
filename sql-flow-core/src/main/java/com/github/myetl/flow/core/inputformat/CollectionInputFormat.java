package com.github.myetl.flow.core.inputformat;


import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.types.Row;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 测试时使用 数组数据集
 */
public class CollectionInputFormat extends FlowInputFormat implements NonParallelInput {

    private TypeSerializer<Row> serializer;

    private transient List<Row> rows;

    private transient Iterator<Row> iterator;

    public CollectionInputFormat(RowTypeInfo rowTypeInfo, List<Row> rows, ExecutionConfig executionConfig) {
        super(rowTypeInfo);
        this.rows = rows;
        this.serializer = rowTypeInfo.createSerializer(executionConfig);
    }

    @Override
    public void open(InputSplit split) throws IOException {

        System.out.println("CollectionInputFormat open " + split.getSplitNumber()+":"+Thread.currentThread().getId());
        this.iterator = this.rows.iterator();
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !this.iterator.hasNext();
    }

    @Override
    public Row nextRecord(Row reuse) throws IOException {
        return this.iterator.next();
    }

    @Override
    public void close() throws IOException {
        this.rows.clear();
    }


    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        final int size = rows.size();
        out.writeInt(size);

        if (size > 0) {
            DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(out);
            for (Row row : rows){
                serializer.serialize(row, wrapper);
            }
        }
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        int collectionLength = in.readInt();
        List<Row> list = new ArrayList<Row>(collectionLength);

        if (collectionLength > 0) {
            try {
                DataInputViewStreamWrapper wrapper = new DataInputViewStreamWrapper(in);
                for (int i = 0; i < collectionLength; i++){
                    Row element = serializer.deserialize(wrapper);
                    list.add(element);
                }
            }
            catch (Throwable t) {
                throw new IOException("Error while deserializing element from collection", t);
            }
        }

        rows = list;
    }



    @Override
    public String toString() {
        return StringUtils.join(this.rows, "\n");
    }

    public static <T> CollectionInputFormatBuilder builder(ExecutionConfig executionConfig, T... dataSet) {
        return new CollectionInputFormatBuilder(dataSet, executionConfig);
    }

    public static class CollectionInputFormatBuilder<T> implements InputFormatBuilder<CollectionInputFormat> {
        private final T[] dataSet;
        private final ExecutionConfig executionConfig;

        public CollectionInputFormatBuilder(T[] dataSet, ExecutionConfig executionConfig) {
            this.dataSet = dataSet;
            this.executionConfig = executionConfig;
        }

        @Override
        public CollectionInputFormat build() {
            if (dataSet == null) {
                throw new IllegalArgumentException("The data must not be null.");
            } else if (dataSet.length == 0) {
                throw new IllegalArgumentException("The number of elements must not be zero.");
            } else {
                TypeInformation typeInfo;
                try {
                    typeInfo = TypeExtractor.getForObject(dataSet[0]);
                } catch (Exception var4) {
                    throw new RuntimeException("Could not create TypeInformation for type " + dataSet[0].getClass().getName() + "; please specify the TypeInformation manually");
                }
                List<Row> rows = new ArrayList<>(dataSet.length);
                RowTypeInfo rowTypeInfo = null;

                if (typeInfo.isBasicType()) {
                    rowTypeInfo = new RowTypeInfo(typeInfo);
                    for (T t : this.dataSet) {
                        Row row = new Row(1);
                        row.setField(0, t);
                        rows.add(row);
                    }
                } else if (typeInfo instanceof CompositeType) {
                    CompositeType pojoTypeInfo = (CompositeType) typeInfo;
                    int total = pojoTypeInfo.getTotalFields();
                    TypeInformation[] types = new TypeInformation[total];
                    for (int i = 0; i < total; i++) {
                        types[i] = pojoTypeInfo.getTypeAt(i);
                    }
                    rowTypeInfo = new RowTypeInfo(types, pojoTypeInfo.getFieldNames());

                    try {
                        for (T t : this.dataSet) {
                            Row row = new Row(total);
                            for (int i = 0; i < total; i++) {
                                row.setField(i, t.getClass().getDeclaredField(pojoTypeInfo.getFieldNames()[i]).get(t));
                            }
                            rows.add(row);
                        }
                    } catch (Exception e) {
                        throw new IllegalArgumentException("CollectionInputFormat type info error:" + e.getMessage(), e);
                    }
                } else if (typeInfo instanceof GenericTypeInfo) {
                    GenericTypeInfo genericTypeInfo = (GenericTypeInfo) typeInfo;
                    Field[] fields = genericTypeInfo.getTypeClass().getDeclaredFields();
                    int total = fields.length;

                    try {
                        TypeInformation[] types = new TypeInformation[total];
                        String[] names = new String[total];

                        for (T t : this.dataSet) {
                            Row row = new Row(total);
                            for (int i = 0; i < total; i++) {

                                fields[i].setAccessible(true);

                                Object v = fields[i].get(t);
                                if (v != null && types[i] == null) {
                                    String name = fields[i].getName();
                                    names[i] = name;
                                    types[i] = TypeExtractor.getForObject(v);
                                }
                                row.setField(i, v);
                            }
                            rows.add(row);
                        }

                        for (int i = 0; i < total; i++) {
                            if (types[i] == null) {
                                types[i] = new GenericTypeInfo<>(String.class);
                            }
                        }
                        rowTypeInfo = new RowTypeInfo(types, names);
                    } catch (Exception e) {
                        throw new IllegalArgumentException("CollectionInputFormat type info error:" + e.getMessage(), e);
                    }
                } else {
                    throw new IllegalArgumentException("CollectionInputFormat TypeInfo not support:" + typeInfo.getClass().getCanonicalName());
                }
                return new CollectionInputFormat(rowTypeInfo, rows, executionConfig);
            }
        }
    }
}