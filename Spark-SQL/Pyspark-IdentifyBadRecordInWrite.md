# Identify Bad Record in Spark Writter

DataFrameWriter handles batch writes and most of the time a single record fails the whole Job and there is no way to identify the bad records which failed to get itself inserted into the external storage system.

In this recipe i try to handle this problem. We create a Dataframe and try to insert into MySQL Table.

- Source Dataset:

| id | name |
|---|---|
|1|san|
|---|---|

- MySQL Table definition:

```
mysql> desc my_data;
+-------+-------------+------+-----+---------+-------+
| Field | Type        | Null | Key | Default | Extra |
+-------+-------------+------+-----+---------+-------+
| id    | int         | NO   | PRI | NULL    |       |
| name  | varchar(10) | YES  |     | NULL    |       |
+-------+-------------+------+-----+---------+-------+
2 rows in set (0.01 sec)

```

- Lets see the solution

