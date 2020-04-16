package com.github.myetl.fiflow.core.flink;

/**
 * 构建级别
 * 1. help list tables 之类的无需执行
 * 2. select 需要特别处理 后 execute
 * 3. insert 可以直接提交执行
 */
public enum BuildLevel {
    None(0),     // 啥也没干
    Show(1),     // help, show tables  之类的无需执行 只要给出信息
    Set(2),      // 设置 jar 并发 等

    Create(3),   // create table
    Select(4),   // select
    Insert(5),   // insert

    Error(11);    // 错误

    public final Integer level;

    BuildLevel(Integer level) {
        this.level = level;
    }

    public static BuildLevel parse(String name) {
        for (BuildLevel v : values()) {
            if (v.name().equalsIgnoreCase(name)) return v;
        }
        return null;
    }

    public boolean isNeedExecute() {
        if (this == BuildLevel.Select || this == BuildLevel.Insert) {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return name();
    }
}
