package com.github.myetl.flow.core.enums;


import org.apache.commons.lang3.StringUtils;

/**
 * 字段类型
 */
public enum FieldType {
    VARCHAR,    // 可变长度字符串	最大容量为4Mb
    BOOLEAN,    // 逻辑值	值：TRUE，FALSE，UNKNOWN
    TINYINT,    // 	微整型，1字节整数	范围是-128到127
    SMALLINT,   //	短整型，2字节整数	范围是-32768到32767
    INT,        // 整型，4字节整数	范围是-2147483648到2147483647
    LONG,       // 长整型
    BIGINT,     //	长整型，8字节整数	范围是-9223372036854775808到9223372036854775807
    FLOAT,      // 	4字节浮点数	6位数字精度
    DECIMAL,    //	小数类型	示例：123.45是DECIMAL（5,2）值。
    DOUBLE,     // 	浮点，8字节浮点数	15位十进制精度
    DATE,       // 	日期类型	示例：DATE'1969-07-20'
    TIME,       // 	时间类型	示例：TIME '20：17：40'
    TIMESTAMP,  // 	时间戳，日期和时间	示例：TIMESTAMP'1969-07-20 20:17:40'
    VARBINARY;  // 	二进制数据	byte[] 数组


    public static FieldType from(String type) {
        if (StringUtils.isEmpty(type))
            throw new IllegalArgumentException("field type is empty");
        return valueOf(type.toUpperCase());
    }
}
