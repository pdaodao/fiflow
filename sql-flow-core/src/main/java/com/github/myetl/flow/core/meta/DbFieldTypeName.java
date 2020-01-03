package com.github.myetl.flow.core.meta;

/**
 * 字段类型名称
 */
public enum DbFieldTypeName {
    String("string"),   // 对于不知道长度的通用字符串类型
    Varchar("varchar"),  // 变长字符串 知道长度

    Tinyint("tinyint"),
    Int("int,integer"),
    Bigint("bigint,int unsigned"),
    Boolean("boolean"),

    Float("float"),
    Decimal("decimal"),

    Datetime("datetime"),
    Timestamp("timestamp");

    public String keys;

    DbFieldTypeName(String keys) {
        this.keys = keys.toLowerCase();
    }
}
