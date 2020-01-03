# sql flow based on flink 

based on apache flink 1.9

## How to use 
``` java 
SqlFlow sqlFlow = SqlFlow.create(true);
    
sqlFlow.jdbc()
        .setDbURL("jdbc:mysql://127.0.0.1:3306/flink")
        .setUsername("root")
        .setPassword("root")
        .setDbName("flink")
        .setDriverName("com.mysql.jdbc.Driver")
        .build()
        .setDbNameUsedInsql("flink");

sqlFlow.sqlUpdate("insert into stuout(name,age,class) " +
        "select name, age, class from student where age > 16");

sqlFlow.execute("sqlflow-demo");
```
VS
```
 CREATE TABLE student ( 
    name VARCHAR, 
    age INT, 
   class VARCHAR 
 ) WITH ( 
    'connector.type' = 'jdbc', 
    'connector.url' = 'jdbc:mysql://127.0.0.1:3306/flink', 
    'connector.table' = 'student', 
    'connector.username' = 'root',
    'connector.password' = 'root'
)

 CREATE TABLE stuout ( 
    name VARCHAR, 
    age INT, 
   class VARCHAR 
 ) WITH ( 
    'connector.type' = 'jdbc', 
    'connector.url' = 'jdbc:mysql://127.0.0.1:3306/flink', 
    'connector.table' = 'stuout', 
    'connector.username' = 'root',
    'connector.password' = 'root'
)

insert into stuout(name,age,class) 
   select name, age, class from student where age > 16

```
