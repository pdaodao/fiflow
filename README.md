# sql flow based on flink 


see demo1.java 

``` 
create table person1( 
    age int, 
    name varchar 
) with ( 
    type='csv', 
    path='dir/person.csv'
); 
                
create table person2( 
    age int, 
    name varchar 
) with ( 
    type='csv', 
    path='dir/person2' 
); 


INSERT INTO person2 
SELECT 
    age, 
    name 
FROM person1 where age > 2; 

```

Thanks 

<a href='#'>flink</a> 
<a href='#'>blink</a> 
<a href='https://github.com/DTStack/flinkStreamSQL'>flinkStreamSQL</a> 