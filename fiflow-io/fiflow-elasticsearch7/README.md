# Elasticsearch7 source and sink 

support elasticsearch version 7.x 

## source 
- 按 shard 分片读取数据
- 支持谓词下推 
- 支持异步纬表join 

## sink
- 支持 append 和 retract 模式 

## 如何使用 

``` 
<dependency>
    <groupId>com.github.lessonone</groupId>
    <artifactId>fiflow-elasticsearch7</artifactId>
    <version>1.10-SNAPSHOT</version>
</dependency>
```

示例 demo代码：  
-  DemoSource.java 
-  DemoDDL.java 
-  DemoStream.java 


### ddl 方式 
``` 
CREATE TABLE stuout ( 
	name VARCHAR, 
	age INT, 
	class VARCHAR
) WITH ( 
    'connector.type' = 'elasticsearch', 
    'connector.hosts' = '127.0.0.1:9200', 
    'connector.index' = 'stuout' 
)
```

### connector 方式 
``` 
ESOptions esOptions = ESOptions.builder()
        .setHosts("127.0.0.1:9200")
        .setIndex("student")
        .build();

Schema tableSchema = new Schema()
        .field("name", DataTypes.STRING())
        .field("age", DataTypes.INT())
        .field("class", DataTypes.STRING());

tEnv.connect(new ES()
        .setEsOptions(esOptions))
        .withSchema(tableSchema)
        .createTemporaryTable("student");
```

### stream api 
``` 
// source function
ESSourceFunction sourceFunction = ESSourceFunction.builder()
        .setEsOptions(es1)
        .setRowTypeInfo(tableSchema)
        .build();

// sink function
ESSinkFunction sinkFunction = ESSinkFunction.builder()
        .setEsOptions(es2)
        .setRowTypeInfo(tableSchema)
        .build();
```