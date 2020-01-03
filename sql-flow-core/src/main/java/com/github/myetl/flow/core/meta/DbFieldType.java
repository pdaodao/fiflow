package com.github.myetl.flow.core.meta;


import java.io.Serializable;

/**
 * 字段类型
 */
public class DbFieldType implements Serializable {
    /**
     * 字段类型名称
     */
    public DbFieldTypeName typeName;

    /**
     * 长度 varchar 的 size decimal 的小数点前的长度
     */
    public Integer size = 10;

    /**
     * 小数点后的位数
     */
    public Integer digits = null;

    public DbFieldType() {
    }

    public DbFieldType(DbFieldTypeName typeName, Integer size, Integer digits) {
        this.typeName = typeName;
        this.size = size;
        this.digits = digits;
    }

    public static DbFieldType create(DbFieldTypeName typeName) {
        return create(typeName, null, null);
    }

    public static DbFieldType create(DbFieldTypeName typeName, Integer size, Integer digits) {
        DbFieldType type = new DbFieldType(typeName, size, digits);
        return type;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(typeName);
        if (size != null && size > 0) {
            sb.append("(").append(size);
            if (digits != null && digits > 0) {
                sb.append(",").append(digits);
            }
            sb.append(")");
        }
        return sb.toString();
    }
}
