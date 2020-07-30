# 基于flink的数据流处理系统 
      基于 apache flink 1.11 
      
   
flink1.11 版本相较前一个版本变化比较大，
特别是sql相关功能趋于完善以前需要自己处理的功能在这个版本里都已经实现,
这里准备把fiflow进行一般大版本重构
           
           
## 自定义 catalog 
 
Flink sql 虽然使用起来方便快捷 但是每次都要写 DDL，所以需要自动获取DDL。 

> Flink 中的数据表管理采用 catalog => database => table 的结构来管理数据表，类似于二级分类，
> 这里的分类只是一个逻辑结构，同一个database下的数据表来自不同的物理数据库。
> 

针对Flink catalog的特点，fiflow设计了两种 catalog 

- FlinkMetaCatalog  
    每个database和一个物理实体数据库绑定，使用到该 database下的表时自动到该物理数据库下获取表结构信息。
    ```
        FlinkMetaCatalog metaCatalog = new FlinkMetaCatalog("meta");
        tEnv.registerCatalog("meta", metaCatalog);
        metaCatalog.addDbInfo("mysqltest",
                new DbInfo().setUrl("jdbc:mysql://127.0.0.1:3306/mysqltest")
                        .setUsername("root")
                        .setPassword("root"));

        tEnv.useCatalog("meta");
        tEnv.useDatabase("mysqltest");

        String insert = "insert into stuout(name, age, class) \n" +
                "   select name, age, class from student where age > 16";
    ```
    
- FlinkInDbCatalog 
    该 Catalog 和 flink 的 catalog 逻辑相同，只是把表结构信息保存在后台数据库中。  
    可以通过 DDL 或者 在 页面上进行操作的方式把数据表结构注册到数据库中。 
    ```
      final String InDbCatalog = "in_db";
        DbInfo dbInfo = new DbInfo()
                .setUrl("jdbc:mysql://127.0.0.1:3306/flink")
                .setUsername("root")
                .setPassword("root")
                .setDriverClassName("com.mysql.cj.jdbc.Driver");

        tEnv.registerCatalog(InDbCatalog, new FlinkInDbCatalog(InDbCatalog, dbInfo));

        tEnv.useCatalog(InDbCatalog);

        String insert = "insert into stuout(name,age,class) \n" +
                "   select name, age, class from student where age > 16";
    ```


## 