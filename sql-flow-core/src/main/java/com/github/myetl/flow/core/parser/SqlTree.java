package com.github.myetl.flow.core.parser;

import java.util.ArrayList;
import java.util.List;

/**
 * sql 解析后的 sql 树
 */
public class SqlTree {

    /**
     * user define functions
     */
    private List<UDF> udfs = new ArrayList<>();

    /**
     * create table
     */
    private List<DDL> ddls = new ArrayList<>();

    /**
     * insert into
     */
    private List<DML> dmls = new ArrayList<>();


    public SqlTree addUdf(UDF udf) {
        this.udfs.add(udf);
        return this;
    }

    public SqlTree addDDL(DDL tableInfo) {
        this.ddls.add(tableInfo);
        return this;
    }

    public SqlTree addDml(DML dml) {
        this.dmls.add(dml);
        return this;
    }

    public List<DDL> getDdls() {
        return ddls;
    }

    public List<UDF> getUdfs() {
        return udfs;
    }

    public List<DML> getDmls() {
        return dmls;
    }
}
