``` 
create table person1( 
        age int, 
        name varchar 
) with ( 
        type='csv', 
        path='sql-flow-core/src/test/resources/person.csv' 
); 

  
create table person2( 
        age int, 
        name varchar
) with ( 
        type='csv',
        path='sql-flow-core/src/test/resources/person2' 
);


INSERT INTO person2 
SELECT 
      age, 
      name 
FROM person1 where age > 2;
```