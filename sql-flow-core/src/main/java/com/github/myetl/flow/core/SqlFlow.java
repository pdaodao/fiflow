package com.github.myetl.flow.core;

import com.github.myetl.flow.core.connect.JdbcProperties;
import com.github.myetl.flow.core.exception.SqlFlowException;
import com.github.myetl.flow.core.frame.FactoryManager;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Flink sql api 封装
 * 简化数据表连接信息
 * 自动读取数据表字段信息
 */
public class SqlFlow {
    public final EnvironmentSettings settings;
    public final TableEnvironment tableEnv;
    public final TableMetaStore dbMetaStore;
    public final List<String> sqlList = new ArrayList<>();


    /**
     * SqlFlow 构造器
     *
     * @param settings    flink EnvironmentSettings
     * @param tableEnv    flink TableEnvironment
     * @param dbMetaStore 元信息存储中心
     */
    public SqlFlow(EnvironmentSettings settings, TableEnvironment tableEnv, TableMetaStore dbMetaStore) {
        this.settings = settings;
        this.tableEnv = tableEnv;
        if (dbMetaStore == null) {
            dbMetaStore = new TableMetaStore();
        }
        this.dbMetaStore = dbMetaStore;
    }

    /**
     * 创建 SqlFlow
     *
     * @param isStreamingMode 是否流处理模式
     * @return
     */
    public static SqlFlow create(boolean isStreamingMode) {
        return create(isStreamingMode, null);
    }

    /**
     * 创建 SqlFlow
     *
     * @param isStreamingMode 是否流处理模式
     * @param dbMetaStore     元信息存储中心
     * @return
     */
    public static SqlFlow create(boolean isStreamingMode, TableMetaStore dbMetaStore) {
        EnvironmentSettings.Builder settingBuilder = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner();
        if (!isStreamingMode) {
            settingBuilder.inBatchMode();
        }
        EnvironmentSettings settings = settingBuilder.build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        SqlFlow sqlFlow = new SqlFlow(settings, tableEnv, dbMetaStore);
        return sqlFlow;
    }

    public Table sqlQuery(String sql) throws SqlParseException, SqlFlowException {
        sqlList.add(sql);
        register(sql);
        return tableEnv.sqlQuery(sql);
    }

    /**
     * 注册 schema 和  source sink
     * @param sql
     */
    private void register(String sql) throws SqlParseException, SqlFlowException {
        dbMetaStore.registerCatalogTableInternal(sql, tableEnv, settings);
    }


    public void sqlUpdate(String sql) throws SqlParseException, SqlFlowException{
        sqlList.add(sql);
        register(sql);
        tableEnv.sqlUpdate(sql);
    }

    public JobExecutionResult execute(String name) throws Exception {
        FactoryManager.closeAll();
        return tableEnv.execute(name);
    }

    public JdbcProperties.JdbcPropertiesBuilder jdbc() {
        return JdbcProperties.builder(dbMetaStore);
    }

    public void kafka() {

    }

    public void elasticsearch() {

    }

    public void file() {

    }
}
