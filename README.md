# Micro DB

## Compile and Run

Assuming that you are in project root folder

To compile `javac -d bin -sourcepath src src/MicroDB.java`  
To Run `java -cp bin MicroDB`  

## Setup

* MicroDB base tables are located in `catalog`
* Indices for unique constraint are `indices` folder
* User tables are `user_data`
* Auto increment sequence files are `sequences` folder

## Features

* List all tables
* Create table
* Supports auto increment with custom seed and increment by
* Unique constraints
* Default values
* Query tables
* Insert into table
* Update to table
* Delete from table
* Drop table
* Create unique constraint after creating table
* Persistent storage between restarts

## Examples to try

### List all tables

  ```SQL
  show tables;
  ```

### Create the table

```sql
  CREATE TABLE test1 (row_id INT PRIMARY KEY, name TEXT NOT NULL, age INT);
  CREATE TABLE test2 (row_id INT PRIMARY KEY, name TEXT, age INT AUTOINCREMENT);
  CREATE TABLE test3 (row_id INT PRIMARY KEY, name TEXT NOT NULL, age INT DEFAULT 1);
  CREATE TABLE test4 (row_id INT PRIMARY KEY, name TEXT NOT NULL, age INT UNIQUE);
```

### Insert the record in the table.

```sql
  INSERT INTO test1 (row_id, name, age)  values (1, 'hearty', 42);
  INSERT INTO test2 (row_id, name)  values (2, 'Alex');
  INSERT INTO test2 (row_id, name)  values (2, 'George');
```

### Display all/specific attribute with/without where condition the values from table

```SQL
  SELECT * FROM test1;
  SELECT name, age FROM test1;
  SELECT row_id, age FROM test1;
  SELECT * FROM test1 WHERE row_id = 1;
  SELECT * FROM test2 WHERE name = 'Alex';
  SELECT row_id, name, age FROM test1 WHERE row_id = 1;
  SELECT name, age FROM test1 WHERE name = 'hearty';
  SELECT * FROM test2;
  SELECT * FROM test3;
```

### Create unique index

```SQL
  CREATE [UNIQUE] INDEX ON test3 (age);
```

### Display the meta info from the column table

```SQL
  SELECT * FROM master_columns;
  SELECT * FROM master_ tables;
```

### Update

```SQL
  SELECT * FROM test1;
  UPDATE test1 SET age = 12 WHERE name = 'hearty';
  SELECT * FROM test1;
```

### Delete

```SQL
  SELECT * FROM test1;
  DELETE FROM test1 WHERE row_id = 1;
  SELECT * FROM test1;
```

### Drop

```SQL
  DROP TABLE test1;
```

### History

To execute recent commands

```SQL
  history;
  !1;
  !2;
```

### For clean exit

```SQL
  EXIT;
  QUIT;
```
