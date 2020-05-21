package com.github.lessonone.fiflow.core.sql;

import com.github.lessonone.fiflow.core.sql.builder.*;
import com.github.lessonone.fiflow.core.sql.builder.demo.DemoElasticsearch;
import com.github.lessonone.fiflow.core.sql.builder.demo.DemoKafka;
import com.github.lessonone.fiflow.core.sql.builder.demo.DemoMysql;
import com.github.lessonone.fiflow.core.sql.builder.demo.DemoMysqlBinlog;
import com.github.lessonone.fiflow.core.sql.builder.frame.*;
import com.github.lessonone.fiflow.core.sql.builder.system.*;

/**
 * sql command 类型
 */
public enum CmdType {
    Help(new HelpBuilder()),
    DemoMysql(new DemoMysql()),
    DemoKafka(new DemoKafka()),
    DemoElasticsearch(new DemoElasticsearch()),
    DemoMysqlBinlog(new DemoMysqlBinlog()),
    Jar(new JarBuilder()),
    Parallelism(new ParallelismBuilder()),
    Flink(new FlinkBuilder()),
    CreateTable(new CreateTableBuilder()),
    Select(new SelectBuilder()),
    InsertInto(new InsertIntoBuilder()),
    InsertOverwrite(new InsertOverwriteBuilder()),

    DropTable(new DropTableBuilder()),
    CreateView(new CreateViewBuilder()),
    DropView(new DropViewBuilder()),
    AlterDatabase(new AlterDatabaseBuilder()),
    CreateDatabase(new CreateDatabaseBuilder()),
    DropDatabase(new DropDatabaseBuilder()),


    // frame
    Set(new SetBuilder()),
    Udf(new UdfBuilder()),

    // system
//    Clear(new ClearBuilder()),
    Describe(new DescribeBuilder()),
    Explain(new ExplainBuilder()),
    Quit(new QuitBuilder()),
    ShowCatalogs(new ShowCatalogsBuilder()),
    ShowDatabases(new ShowDatabasesBuilder()),
    ShowFunctions(new ShowFunctionsBuilder()),
    ShowModules(new ShowModulesBuilder()),
    ShowTables(new ShowTablesBuilder()),
    Use(new UseBuilder()),
    UseCatalog(new UseCatalogBuilder());


    public final CmdBuilder cmdBuilder;

    CmdType(CmdBuilder cmdBuilder) {
        this.cmdBuilder = cmdBuilder;
    }
}
