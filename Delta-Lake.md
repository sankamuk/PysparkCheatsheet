# Delta Lake (OSS)

## Documentations 

https://docs.delta.io/latest/quick-start.html

https://docs.delta.io/latest/releases.html


## Install 

```
(mypython) apple@apples-MacBook-Air ~ % pip install pyspark  

(mypython) apple@apples-MacBook-Air ~ % pip freeze | grep -i spark
You are using pip version 10.0.1, however version 21.1.3 is available.
You should consider upgrading via the 'pip install --upgrade pip' command.
pyspark==3.1.2

pyspark --packages io.delta:delta-core_2.12:1.0.0 \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    --conf "park.sql.warehouse.dir=/Users/apple/TEST/DELTA_LAKE/warehouse"
```

- `Creating Local Storage for data`

```
(mypython) apple@apples-MacBook-Air DELTA_LAKE % pwd
/Users/apple/TEST/DELTA_LAKE
apple@apples-MacBook-Air DELTA_LAKE % ls -ltr        
total 0
drwxr-xr-x  2 apple  staff   64 Jun 27 16:08 raw
drwxr-xr-x  2 apple  staff   64 Jun 27 16:08 stage
drwxr-xr-x  4 apple  staff  128 Jun 27 19:43 delta
drwxr-xr-x  2 apple  staff   64 Jun 27 21:00 warehouse
```

## Get warehouse directory

>>> spark.conf.get('spark.sql.warehouse.dir')
'file:/Users/apple/spark-warehouse'

## Basic Operations 


- `Create dataframe`

```
df1 = spark.createDataFrame([('san', 32, 3000, 'it'), ('kun', 31, 4500, 'it'), ('jad', 33, 4000, 'it'), ('pascal', 40, 5000, 'mng')], ['name', 'age', 'salary', 'dept'])
>>> df1.rdd.getNumPartitions()
4
>>> df1 = df1.repartition(1)
>>> df1.rdd.getNumPartitions()
1
```

- `Save as delta table`

```
df1.write.format('delta').save('/Users/apple/TEST/DELTA_LAKE/delta/emp')

apple@apples-MacBook-Air DELTA_LAKE % ls -ltr /Users/apple/TEST/DELTA_LAKE/delta/emp                                 
total 8
-rw-r--r--  1 apple  staff  1158 Jun 27 19:41 part-00000-ae3f833c-e7b1-485d-b363-35a0e2e51719-c000.snappy.parquet
drwxr-xr-x  3 apple  staff    96 Jun 27 19:41 _delta_log
apple@apples-MacBook-Air DELTA_LAKE % ls -ltr /Users/apple/TEST/DELTA_LAKE/delta/emp/_delta_log                      
total 8
-rw-r--r--  1 apple  staff  976 Jun 27 19:41 00000000000000000000.json
apple@apples-MacBook-Air DELTA_LAKE % cat /Users/apple/TEST/DELTA_LAKE/delta/emp/_delta_log/00000000000000000000.json
{"commitInfo":{"timestamp":1624803078562,"operation":"WRITE","operationParameters":{"mode":"ErrorIfExists","partitionBy":"[]"},"isBlindAppend":true,"operationMetrics":{"numFiles":"1","numOutputBytes":"1158","numOutputRows":"4"}}}
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"b93af26e-f54a-415b-b4d0-228b0f40b553","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"age\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"salary\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"dept\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1624803078229}}
{"add":{"path":"part-00000-ae3f833c-e7b1-485d-b363-35a0e2e51719-c000.snappy.parquet","partitionValues":{},"size":1158,"modificationTime":1624803078000,"dataChange":true}}
```

- `Save as delta table with partition`

```
df1.write.format('delta').partitionBy('dept').save('/Users/apple/TEST/DELTA_LAKE/delta/emp_part')

apple@apples-MacBook-Air DELTA_LAKE % ls -ltr /Users/apple/TEST/DELTA_LAKE/delta/emp_part                            
total 0
drwxr-xr-x  4 apple  staff  128 Jun 27 19:43 dept=it
drwxr-xr-x  4 apple  staff  128 Jun 27 19:43 dept=mng
drwxr-xr-x  3 apple  staff   96 Jun 27 19:43 _delta_log
apple@apples-MacBook-Air DELTA_LAKE % ls -ltr /Users/apple/TEST/DELTA_LAKE/delta/emp_part/dept=it 
total 8
-rw-r--r--  1 apple  staff  929 Jun 27 19:43 part-00000-55d0f536-c895-4190-a70c-661ee24a5a99.c000.snappy.parquet
apple@apples-MacBook-Air DELTA_LAKE % ls -ltr /Users/apple/TEST/DELTA_LAKE/delta/emp_part/_delta_log 
total 8
-rw-r--r--  1 apple  staff  1199 Jun 27 19:43 00000000000000000000.json
apple@apples-MacBook-Air DELTA_LAKE % cat /Users/apple/TEST/DELTA_LAKE/delta/emp_part/_delta_log/00000000000000000000.json
{"commitInfo":{"timestamp":1624803232734,"operation":"WRITE","operationParameters":{"mode":"ErrorIfExists","partitionBy":"[\"dept\"]"},"isBlindAppend":true,"operationMetrics":{"numFiles":"2","numOutputBytes":"1866","numOutputRows":"4"}}}
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"93e065ec-9216-4dcd-8fba-fd984f64ff45","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"age\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"salary\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"dept\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["dept"],"configuration":{},"createdTime":1624803232281}}
{"add":{"path":"dept=it/part-00000-55d0f536-c895-4190-a70c-661ee24a5a99.c000.snappy.parquet","partitionValues":{"dept":"it"},"size":929,"modificationTime":1624803232000,"dataChange":true}}
{"add":{"path":"dept=mng/part-00000-e256df2a-4229-479b-9c19-3fb90369298a.c000.snappy.parquet","partitionValues":{"dept":"mng"},"size":937,"modificationTime":1624803232000,"dataChange":true}}
```

- `Read delta table`

```
>>> spark.read.format('delta').load('/Users/apple/TEST/DELTA_LAKE/delta/emp').show()
+------+---+------+----+
|  name|age|salary|dept|
+------+---+------+----+
|   san| 32|  3000|  it|
|   kun| 31|  4500|  it|
|   jad| 33|  4000|  it|
|pascal| 40|  5000| mng|
+------+---+------+----+

>>> spark.read.format('delta').load('/Users/apple/TEST/DELTA_LAKE/delta/emp_part').show()
+------+---+------+----+
|  name|age|salary|dept|
+------+---+------+----+
|pascal| 40|  5000| mng|
|   san| 32|  3000|  it|
|   kun| 31|  4500|  it|
|   jad| 33|  4000|  it|
+------+---+------+----+

>>> spark.sql('select * from delta.`/Users/apple/TEST/DELTA_LAKE/delta/emp_part`').show()
21/06/27 19:59:22 WARN ObjectStore: Failed to get database delta, returning NoSuchObjectException
+------+---+------+----+
|  name|age|salary|dept|
+------+---+------+----+
|pascal| 40|  5000| mng|
|   san| 32|  3000|  it|
|   kun| 31|  4500|  it|
|   jad| 33|  4000|  it|
+------+---+------+----+
```

- `Update delta table`

```
>>> df2.write.format('delta').mode('append').save('/Users/apple/TEST/DELTA_LAKE/delta/emp')
>>> spark.read.format('delta').load('/Users/apple/TEST/DELTA_LAKE/delta/emp').show()
+------+---+------+----+
|  name|age|salary|dept|
+------+---+------+----+
|   san| 32|  3000|  it|
|   kun| 31|  4500|  it|
|   jad| 33|  4000|  it|
|pascal| 40|  5000| mng|
|   xun| 30|  4000|  it|
+------+---+------+----+

apple@apples-MacBook-Air DELTA_LAKE % ls -ltr /Users/apple/TEST/DELTA_LAKE/delta/emp                                 
total 24
-rw-r--r--  1 apple  staff  1158 Jun 27 19:41 part-00000-ae3f833c-e7b1-485d-b363-35a0e2e51719-c000.snappy.parquet
-rw-r--r--  1 apple  staff   533 Jun 27 19:51 part-00000-ad9df370-60fa-42fa-84f7-d17a8c304c29-c000.snappy.parquet
-rw-r--r--  1 apple  staff  1095 Jun 27 19:51 part-00003-e1de7ed4-ee0c-4aa2-830c-fcdf48579863-c000.snappy.parquet
drwxr-xr-x  4 apple  staff   128 Jun 27 19:51 _delta_log
apple@apples-MacBook-Air DELTA_LAKE % cat /Users/apple/TEST/DELTA_LAKE/delta/emp/_delta_log/00000000000000000001.json 
{"commitInfo":{"timestamp":1624803704812,"operation":"WRITE","operationParameters":{"mode":"Append","partitionBy":"[]"},"readVersion":0,"isBlindAppend":true,"operationMetrics":{"numFiles":"2","numOutputBytes":"1628","numOutputRows":"1"}}}
{"add":{"path":"part-00000-ad9df370-60fa-42fa-84f7-d17a8c304c29-c000.snappy.parquet","partitionValues":{},"size":533,"modificationTime":1624803704000,"dataChange":true}}
{"add":{"path":"part-00003-e1de7ed4-ee0c-4aa2-830c-fcdf48579863-c000.snappy.parquet","partitionValues":{},"size":1095,"modificationTime":1624803704000,"dataChange":true}}
```

- `Overwrite`

```
df2.write.format('delta').mode('overwrite').save('/Users/apple/TEST/DELTA_LAKE/delta/emp_part')
>>> df2.write.format('delta').mode('overwrite').save('/Users/apple/TEST/DELTA_LAKE/delta/emp_part')
>>> spark.read.format('delta').load('/Users/apple/TEST/DELTA_LAKE/delta/emp_part').show()
+----+---+------+----+
|name|age|salary|dept|
+----+---+------+----+
| xun| 30|  4000|  it|
+----+---+------+----+

apple@apples-MacBook-Air DELTA_LAKE % ls -ltr /Users/apple/TEST/DELTA_LAKE/delta/emp_part                                     
total 0
drwxr-xr-x  4 apple  staff  128 Jun 27 19:43 dept=mng
drwxr-xr-x  6 apple  staff  192 Jun 27 19:53 dept=it
drwxr-xr-x  4 apple  staff  128 Jun 27 19:53 _delta_log
apple@apples-MacBook-Air DELTA_LAKE % ls -ltr /Users/apple/TEST/DELTA_LAKE/delta/emp_part/_delta_log                          
total 16
-rw-r--r--  1 apple  staff  1199 Jun 27 19:43 00000000000000000000.json
-rw-r--r--  1 apple  staff   875 Jun 27 19:53 00000000000000000001.json
apple@apples-MacBook-Air DELTA_LAKE % cat /Users/apple/TEST/DELTA_LAKE/delta/emp_part/_delta_log/00000000000000000001.json 
{"commitInfo":{"timestamp":1624803781580,"operation":"WRITE","operationParameters":{"mode":"Overwrite","partitionBy":"[]"},"readVersion":0,"isBlindAppend":false,"operationMetrics":{"numFiles":"1","numOutputBytes":"906","numOutputRows":"1"}}}
{"add":{"path":"dept=it/part-00003-9b147e73-0b1c-46a5-bd26-4aa59fcfb3fd.c000.snappy.parquet","partitionValues":{"dept":"it"},"size":906,"modificationTime":1624803781000,"dataChange":true}}
{"remove":{"path":"dept=it/part-00000-55d0f536-c895-4190-a70c-661ee24a5a99.c000.snappy.parquet","deletionTimestamp":1624803781580,"dataChange":true,"extendedFileMetadata":true,"partitionValues":{"dept":"it"},"size":929}}
{"remove":{"path":"dept=mng/part-00000-e256df2a-4229-479b-9c19-3fb90369298a.c000.snappy.parquet","deletionTimestamp":1624803781580,"dataChange":true,"extendedFileMetadata":true,"partitionValues":{"dept":"mng"},"size":937}}
```

- `Save data as table`

```
df1.write.format('delta').saveAsTable('emp_meta')

apple@apples-MacBook-Air DELTA_LAKE % ls -ltr ~/spark-warehouse/emp_meta 
total 8
-rw-r--r--  1 apple  staff  1158 Jun 27 21:07 part-00000-d58294e0-42d4-4aa3-bc22-e7d149b00fdf-c000.snappy.parquet
drwxr-xr-x  3 apple  staff    96 Jun 27 21:07 _delta_log

>>> spark.sql('select * from emp_meta').show()
+------+---+------+----+
|  name|age|salary|dept|
+------+---+------+----+
|   san| 32|  3000|  it|
|   kun| 31|  4500|  it|
|   jad| 33|  4000|  it|
|pascal| 40|  5000| mng|
+------+---+------+----+
```

> NOTE: you can only save in location specified my warehouse location.

- `Define table on a already created dataset`

```
spark.sql("create table emp (name string, age bigint, salary bigint, dept string) using delta location '/Users/apple/TEST/DELTA_LAKE/delta/emp'") 

>>> spark.sql('select * from emp').show()                                                                                  
+------+---+------+----+
|  name|age|salary|dept|
+------+---+------+----+
|   san| 32|  3000|  it|
|   kun| 31|  4500|  it|
|   jad| 33|  4000|  it|
|pascal| 40|  5000| mng|
|   xun| 30|  4000|  it|
+------+---+------+----+

spark.sql("create table emp_part (name string, age bigint, salary bigint, dept string) using delta partitioned by (dept) location '/Users/apple/TEST/DELTA_LAKE/delta/emp_part'") 

>>> spark.sql('select * from emp_part').show()                                                                                  +----+---+------+----+
|name|age|salary|dept|
+----+---+------+----+
| xun| 30|  4000|  it|
+----+---+------+----+
```


## Delta Transaction Log 


- `Look into history`

```
from delta.tables import *

dt = DeltaTable.forPath(spark, '/Users/apple/TEST/DELTA_LAKE/delta/emp_part')
OR
dt = DeltaTable.forName(spark, 'emp_part')

dt.history().show()
dt.history(1).show()
>>> dt.history(1).select('operationMetrics').show()
+--------------------+
|    operationMetrics|
+--------------------+
|{numFiles -> 1, n...|
+--------------------+

spark.sql('describe history delta.`/Users/apple/TEST/DELTA_LAKE/delta/emp_part`').show()
```

- `Removing history (dry run)`

> Default retaintion is 7 days, delete history less than 200 hours needs 'spark.databricks.delta.retentionDurationCheck.enabled = false' to be set.

```
>>> spark.sql('vacuum emp_part retain 200 hours dry run')
Found 0 files and directories in a total of 3 directories that are safe to delete.
DataFrame[path: string] 
```

> Note: Dry run in non sql is not yet available

- `Removing history`

```
>>> dt.vacuum(200)
Deleted 0 files and directories in a total of 3 directories.                    
DataFrame[]
>>> dt.vacuum(200).show()
Deleted 0 files and directories in a total of 3 directories.                    
++
||
++
++

>>> spark.sql('vacuum emp_part retain 200 hours').show()
Deleted 0 files and directories in a total of 3 directories.                    
+--------------------+
|                path|
+--------------------+
|file:/Users/apple...|
+--------------------+
```

- `Configuration`

    - ***spark.databricks.delta.deletedFileRetentionDuration*** - how long ago the files were marked for deletion before they are deleted by VACUUM
    - ***spark.databricks.delta.logRetentionDuration (30)*** - How much history your Delta table retains is configurable per table
    - ***spark.databricks.delta.vacuum.parallelDelete.enabled*** - using VACUUM, to configure Spark to delete files in parallel

- `Describe`

```
>>> spark.sql('describe detail delta.`/Users/apple/TEST/DELTA_LAKE/delta/emp_part`').show()
21/06/28 10:42:17 WARN ObjectStore: Failed to get database delta, returning NoSuchObjectException
+------+--------------------+----+-----------+--------------------+--------------------+-------------------+----------------+--------+-----------+----------+----------------+----------------+
|format|                  id|name|description|            location|           createdAt|       lastModified|partitionColumns|numFiles|sizeInBytes|properties|minReaderVersion|minWriterVersion|
+------+--------------------+----+-----------+--------------------+--------------------+-------------------+----------------+--------+-----------+----------+----------------+----------------+
| delta|93e065ec-9216-4dc...|null|       null|file:/Users/apple...|2021-06-27 19:43:...|2021-06-27 19:53:01|          [dept]|       1|        906|        {}|               1|               2|
+------+--------------------+----+-----------+--------------------+--------------------+-------------------+----------------+--------+-----------+----------+----------------+----------------+


>>> spark.sql('describe table delta.`/Users/apple/TEST/DELTA_LAKE/delta/emp_part`').show()
21/06/28 10:43:54 WARN ObjectStore: Failed to get database delta, returning NoSuchObjectException
+--------------+---------+-------+
|      col_name|data_type|comment|
+--------------+---------+-------+
|          name|   string|       |
|           age|   bigint|       |
|        salary|   bigint|       |
|          dept|   string|       |
|              |         |       |
|# Partitioning|         |       |
|        Part 0|     dept|       |
+--------------+---------+-------+

>>> spark.sql('describe table extended delta.`/Users/apple/TEST/DELTA_LAKE/delta/emp_part`').show()
21/06/28 10:44:29 WARN ObjectStore: Failed to get database delta, returning NoSuchObjectException
+--------------------+--------------------+-------+
|            col_name|           data_type|comment|
+--------------------+--------------------+-------+
|                name|              string|       |
|                 age|              bigint|       |
|              salary|              bigint|       |
|                dept|              string|       |
|                    |                    |       |
|      # Partitioning|                    |       |
|              Part 0|                dept|       |
|                    |                    |       |
|# Detailed Table ...|                    |       |
|                Name|delta.`file:/User...|       |
|            Location|/Users/apple/TEST...|       |
|            Provider|               delta|       |
|    Table Properties|[delta.minReaderV...|       |
+--------------------+--------------------+-------+
```

- `Generate Manifest`

For other system, i.e. Presto, etc to query Delta table you can generate manifest files.

- `Existing parquet table to delta table`

```
>>> spark.sql('select * from emp').show()
+------+---+------+----+                                                        
|  name|age|salary|dept|
+------+---+------+----+
|   san| 32|  3000|  it|
|   kun| 31|  4500|  it|
|   jad| 33|  4000|  it|
|pascal| 40|  5000| mng|
|   xun| 30|  4000|  it|
+------+---+------+----+

>>> df = spark.sql('select * from emp')
>>> df.write.format('parquet').partitionBy('dept').save('/Users/apple/TEST/DELTA_LAKE/raw/emp')

apple@apples-MacBook-Air DELTA_LAKE % pwd   
/Users/apple/TEST/DELTA_LAKE
apple@apples-MacBook-Air DELTA_LAKE % ls -ltr raw/emp 
total 0
drwxr-xr-x  4 apple  staff  128 Jun 29 08:48 dept=mng
drwxr-xr-x  6 apple  staff  192 Jun 29 08:48 dept=it
-rw-r--r--  1 apple  staff    0 Jun 29 08:48 _SUCCESS

>>> spark.sql('convert to delta parquet.`/Users/apple/TEST/DELTA_LAKE/raw/emp` partitioned by (dept string)')
21/06/29 08:51:48 WARN ObjectStore: Failed to get database parquet, returning NoSuchObjectException
DataFrame[]

apple@apples-MacBook-Air DELTA_LAKE % ls -ltr raw/emp
total 0
drwxr-xr-x  4 apple  staff  128 Jun 29 08:48 dept=mng
drwxr-xr-x  6 apple  staff  192 Jun 29 08:48 dept=it
-rw-r--r--  1 apple  staff    0 Jun 29 08:48 _SUCCESS
drwxr-xr-x  6 apple  staff  192 Jun 29 08:51 _delta_log

>>> df.write.format('parquet').partitionBy('dept').save('/Users/apple/TEST/DELTA_LAKE/raw/emp_p')    
>>> from delta.tables import *
>>> DeltaTable.convertToDelta(spark, "parquet.`/Users/apple/TEST/DELTA_LAKE/raw/emp_p`", "dept string")
21/06/29 08:55:27 WARN ObjectStore: Failed to get database parquet, returning NoSuchObjectException
21/06/29 08:55:30 WARN ObjectStore: Failed to get database parquet, returning NoSuchObjectException
JavaObject id=o96

apple@apples-MacBook-Air DELTA_LAKE % ls -ltr raw/emp_p 
total 0
drwxr-xr-x  4 apple  staff  128 Jun 29 08:53 dept=mng
drwxr-xr-x  6 apple  staff  192 Jun 29 08:53 dept=it
-rw-r--r--  1 apple  staff    0 Jun 29 08:53 _SUCCESS
drwxr-xr-x  6 apple  staff  192 Jun 29 08:55 _delta_log
```

- `Restore Table`
 
```
>>> dlt_tbl = spark.read.format("delta").option("versionAsOf", 0).load("/Users/apple/TEST/DELTA_LAKE/delta/emp")
>>> dlt_tbl.createOrReplaceTempView('emp_0') 
>>> spark.sql('select * from emp').show()                                                        >>> spark.sql('select * from emp').show()
+------+---+------+----+
|  name|age|salary|dept|
+------+---+------+----+
|   san| 32|  3000|  it|
|   kun| 31|  4500|  it|
|   jad| 33|  4000|  it|
|pascal| 40|  5000| mng|
|   xun| 30|  4000|  it|
+------+---+------+----+

>>> spark.sql('select * from emp_0').show()
+------+---+------+----+
|  name|age|salary|dept|
+------+---+------+----+
|   san| 32|  3000|  it|
|   kun| 31|  4500|  it|
|   jad| 33|  4000|  it|
|pascal| 40|  5000| mng|
+------+---+------+----+

>>> spark.sql('insert overwrite emp select * from emp_0')
DataFrame[]                                                                     
>>> spark.sql('select * from emp').show()
+------+---+------+----+
|  name|age|salary|dept|
+------+---+------+----+
|   san| 32|  3000|  it|
|   kun| 31|  4500|  it|
|   jad| 33|  4000|  it|
|pascal| 40|  5000| mng|
+------+---+------+----+

>>> spark.sql('select * from emp_0').show()
+------+---+------+----+
|  name|age|salary|dept|
+------+---+------+----+
|   san| 32|  3000|  it|
|   kun| 31|  4500|  it|
|   jad| 33|  4000|  it|
|pascal| 40|  5000| mng|
+------+---+------+----+
```

