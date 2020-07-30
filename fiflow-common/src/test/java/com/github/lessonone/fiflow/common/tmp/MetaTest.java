package com.github.lessonone.fiflow.common.tmp;

import com.github.lessonone.fiflow.common.base.DbInfo;
import com.github.lessonone.fiflow.common.base.TableInfo;
import com.github.lessonone.fiflow.common.meta.JdbcBaseMetaReader;

public class MetaTest {
    public static void main(String[] args) {
        DbInfo dbInfo = new DbInfo()
                .setUrl("jdbc:mysql://10.12.102.110:3306/flink")
                .setUsername("root")
                .setPassword("root")
                .setDriverClassName("com.mysql.cj.jdbc.Driver");

        JdbcBaseMetaReader jdbcBaseMetaReader = new JdbcBaseMetaReader(dbInfo);

        String info = jdbcBaseMetaReader.info();

        System.out.println(info);

//        System.out.println(jdbcBaseMetaReader.getCatalog());
//
//        System.out.println(jdbcBaseMetaReader.getSchema());
//
//        jdbcBaseMetaReader.listSchemas();

//        System.out.println(StringUtils.join(jdbcBaseMetaReader.listTables(), ","));

        TableInfo tableInfo = jdbcBaseMetaReader.getTable("fi_flink_table");

        System.out.println(tableInfo.toString());

    }
}
