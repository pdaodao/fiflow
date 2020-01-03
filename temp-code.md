check("create database db1 comment 'test create database'" +
			"with ( 'key1' = 'value1', 'key2.a' = 'value2.a')",
			"CREATE DATABASE `DB1`\n" +
			"COMMENT 'test create database' WITH (\n" +
			"  'key1' = 'value1',\n" +
			"  'key2.a' = 'value2.a'\n" +
			")");
			
	create database db1 comment 'test create database'
    with ( 
    	'key1' = 'value1', 
    	'key2.a' = 'value2.a'
    )		