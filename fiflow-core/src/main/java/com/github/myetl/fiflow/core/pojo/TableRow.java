package com.github.myetl.fiflow.core.pojo;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * 表格的一行数据
 */
public class TableRow extends ArrayList<Object> implements Serializable {

    public TableRow(int initialCapacity) {
        super(initialCapacity);
    }

    public TableRow() {
    }

    public static TableRow empty() {
        return new TableRow();
    }

    public static TableRow empty(int size) {
        return new TableRow(size);
    }

    public static TableRow of(Object... values) {
        TableRow row = new TableRow(values.length);
        for (Object v : values) {
            row.add(v);
        }
        return row;
    }

    @Override
    public String toString() {
        Iterator<Object> it = iterator();
        if (!it.hasNext())
            return StringUtils.EMPTY;

        StringBuilder sb = new StringBuilder();
        for (; ; ) {
            Object e = it.next();
            sb.append(e);
            if (!it.hasNext()) {
                return sb.toString();
            }
            sb.append(',').append(' ');
        }
    }
}
