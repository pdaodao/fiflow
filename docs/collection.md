``` 
create table person(
 id varchar,
 name varchar,
 city varchar,
 age int
) with (
 type='collection',
 data='1,小明,1,18;2,小花,2,19;3,张三,3,20;4,李四,4,21'
);


create table city(
 id varchar,
 name varchar
) with (
 type='collection',
 data='1,北京;2,上海;3,杭州;4,香港'
);


create table fullinfo(
 name varchar,
 age int,
 city varchar
) with (
  type='collection'
);



INSERT INTO fullinfo
SELECT
   name,
   age,
   cityname
FROM
   (SELECT a.name as name , 
           a.age as age, 
           b.name as cityname 
    FROM person AS a 
    LEFT JOIN city AS b on a.city = b.id
   ); 
```