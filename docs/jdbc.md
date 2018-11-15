
## jdbc 
``` 
create table person(
 id varchar,
 name varchar,
 city varchar,
 age int 
) with (
 type='jdbc',
 driver='com.mysql.jdbc.Driver',    --数据库驱动
 url='jdbc:mysql://127.0.0.1:3306/hello', --url 
 username='root',                         --用户名
 password='root'                          --密码
);
 
```

## mysql 

``` 
create table person(
 id varchar,
 name varchar,
 city varchar,
 age int 
) with (
 type='mysql',
 url='jdbc:mysql://127.0.0.1:3306/hello', --url 
 username='root',                         --用户名
 password='root'                          --密码
);
```