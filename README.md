# sql flow based on flink 

use sql to run flink job 

## how to use 

``` 
String sql = "..."; 
Flow flow = new Flow(sql);
flow.execute();
```



## support source/sink type 
|          name                        |    demo            |   status   | 
| :-----------------------------------:|:------------------:|:----------:|
| [collection](docs/collection.md)     | CollectionDemo.java|   complete | 
| [csv](docs/csv.md)                   |   CsvDemo.java     |   complete | 
| [jdbc](docs/jdbc.md)                 |                    |   doing    | 
|   elasticsearch                      |                    |   doing    | 
|   kafka                              |                    |   todo     | 
|   hbase                              |                    |   todo     | 


## flink run mode
*  local        support 
*  standalone    todo 
*  yarn          todo 


## web ui todo 


## Thanks  
  <a href='#'>flink</a>    
  
  <a href='#'>blink</a>    
  
  <a href='https://github.com/DTStack/flinkStreamSQL'>flinkStreamSQL</a> 
