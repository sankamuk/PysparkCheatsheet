## Spark SQL

### General Operations

> DataFrame is essentially a type alias for Dataset[Row], where a Row is a generic untyped JVM object.

### 1. Creating DataFrames 

- Method 1 : From RDD

>>> kv_rdd = sc.parallelize(range(1, 20)).map(lambda x: (x, random.random()))       
>>> kv_df = kv_rdd.toDF(['i', 'v'])


>>> kv_df.printSchema()
root
 |-- i: long (nullable = true)
 |-- v: double (nullable = true)

>>> kv_df.show(5)
+---+-------------------+
|  i|                  v|
+---+-------------------+
|  1| 0.4209297142526559|
|  2| 0.7593478572653389|
|  3| 0.8268261936382886|
|  4| 0.2311995074706913|
|  5|  0.886405966799853|
+---+-------------------+

>>> kv_df.count()
19

- Method 2: From RDD with explicit Schema

>>> from pyspark.sql.types import StructType, StructField, StringType, LongType

>>> schma_1 = StructType([StructField('name', StringType()), StructField('salary', LongType())])
>>> names = ['san', 'jad', 'kun', 'xun']
>>> user_rdd = sc.parallelize(names).map(lambda x: (x, random.randint(1000, 5000)))

>>> user_df = spark.createDataFrame(user_rdd, schma_1)
>>> user_df.show()
+----+------+
|name|salary|
+----+------+
| san|  4715|
| jad|  1830|
| kun|  2345|
| xun|  1633|
+----+------+

- Method 3: From Datasource

spark.read.format(...).option("key", value").schema(...).load()

format = json, parquet, jdbc, orc, csv, text
schema = parquet and orc need no schema, but for json, csv the schema can be infered by Spark (may not be correct)

spark.read.json("<path>")
spark.read.format("json")

spark.read.parquet("<path>")
spark.read.format("parquet")

spark.read.jdbc
spark.read.format("jdbc")

spark.read.orc("<path>")
spark.read.format("orc")

spark.read.csv("<path>")
spark.read.format("csv")

spark.read.text("<path>")
spark.read.format("text")

> In case you have created your custom datasource it will be like:
> spark.read.format("muk.san.custom_source")

- Text

(mypython) apples-MacBook-Air:TEST apple$ cat file.txt 
San 32 5000
Jade 31 5500
Kun 30 6000

>>> user_df = spark.read.text('/Users/apple/TEST/file.txt')
>>> user_df.printSchema()
root
 |-- value: string (nullable = true)

>>> user_df.count()
3
>>> user_df.show()
+------------+
|       value|
+------------+
| San 32 5000|
|Jade 31 5500|
| Kun 30 6000|
+------------+


- CSV

(mypython) apples-MacBook-Air:pyspark apple$ echo "name,age,salary" > emp.csv ; cat file.txt | tr " " "," >> emp.csv

>>> emp_df = spark.read.option('header','true').option('sep', ',').csv('/Users/apple/TEST/pyspark/emp.csv')
>>> emp_df.printSchema()
root
 |-- name: string (nullable = true)
 |-- age: string (nullable = true)
 |-- salary: string (nullable = true)

>>> schma_emp = StructType([StructField('name', StringType()), StructField('age', LongType()), StructField('salary', LongType())])
>>> emp_df = spark.read.option('header','true').option('sep', ',').schema(schma_emp).csv('/Users/apple/TEST/pyspark/emp.csv') >>> emp_df.printSchema()                                                                                                      root
 |-- name: string (nullable = true)
 |-- age: long (nullable = true)
 |-- salary: long (nullable = true)

- JSON

> Use samplingRatio to avoid Spark do the sampling for the whole file. Rather only sample portion of the file for Schema inference.

(mypython) apples-MacBook-Air:pyspark apple$ cat emp.json 
[{ 'name': 'San',
'age': 32,
'salary' : 5000 },
{ 'name': 'Jade',
'age': 31,
'salary' : 5500 },
{ 'name': 'Kun',
'age': 30,
'salary' : 6000 }]


>>> emp_df = spark.read.option('samplingRatio', 0.5).json('/Users/apple/TEST/pyspark/emp.json')
>>> emp_df.show()
Traceback (most recent call last):
  File "/Users/apple/TEST/spark/spark-kubernetes-master/BUILD_BINARY/spark/spark2/python/pyspark/sql/utils.py", line 63, in deco
    return f(*a, **kw)
  File "/Users/apple/TEST/spark/spark-kubernetes-master/BUILD_BINARY/spark/spark2/python/lib/py4j-0.10.7-src.zip/py4j/protocol.py", line 328, in get_return_value
...

>>> emp_df = spark.read.option('samplingRatio', 0.5).option('multiLine', 'true').json('/Users/apple/TEST/pyspark/emp.json')
>>> emp_df.show()
+--------------------+
|                 emp|
+--------------------+
|[[32, San, 5000],...|
+--------------------+

(mypython) apples-MacBook-Air:pyspark apple$ cat emp.json 
[{ 'name': 'San',
'age': '32',
'salary' : '5000' },
{ 'name': 'Jade',
'age': '31',
'salary' : '5500' },
{ 'name': 'Kun',
'age': '30',
'salary' : 6000 }]

>>> emp_df = spark.read.option('samplingRatio', 0.5).option('multiLine', 'true').json('/Users/apple/TEST/pyspark/emp.json')
>>> emp_df.show()
+---+----+------+
|age|name|salary|
+---+----+------+
| 32| San|  5000|
| 31|Jade|  5500|
| 30| Kun|  6000|
+---+----+------+


>>> emp_df.printSchema()
root
 |-- age: string (nullable = true)
 |-- name: string (nullable = true)
 |-- salary: string (nullable = true)

>>> emp_df = spark.read.option('multiLine', 'true').schema(schma_emp).json('/Users/apple/TEST/pyspark/emp.json') 
>>> emp_df.printSchema()
root
 |-- name: string (nullable = true)
 |-- age: long (nullable = true)
 |-- salary: long (nullable = true)


> If Spark fail Row parsing with schema, it sets all value of Row as null. In case you can ask Spark to fail with option option("mode","failFast"). Spark will throw RuntimeException.


- Parquet

> Its popularity is because it is a self-describing data format and it stores data columnar storage format and in a highly compact structure by leveraging compressions. 


>>> emp_df_pq = spark.read.parquet('/Users/apple/TEST/pyspark/emp.parquet')
>>> emp_df_pq.show()
+----+---+------+
|name|age|salary|
+----+---+------+
| San| 32|  5000|
|Jade| 31|  5500|
| Kun| 30|  6000|
+----+---+------+

>>> emp_df_pq.printSchema()
root
 |-- name: string (nullable = true)
 |-- age: long (nullable = true)
 |-- salary: long (nullable = true)


- JDBC

os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /data/mysql/mysql-connector-java-5.1.48/mysql-connector-java-5.1.48.jar pyspark-shell'

df_mysql = spark.read.format("jdbc")
             .option("url", "jdbc:mysql://localhost/userdb")
             .option("driver", "com.mysql.jdbc.Driver")
             .option("dbtable", "usertable")
             .option("user", "root")
             .option("password", "Admin@123")
             .load()


df_mysql.show()
+----+---+------+
|name|age|salary|
+----+---+------+
| San| 32|  5000|
|Jade| 31|  5500|
| Kun| 30|  6000|
+----+---+------+


### 2. DataFrames Transformation

> Column operation are of three catagory (a) Mathematical operations such as addition, multiplication, etc (b) Logical comparisons between a column value or a literal such as equality, etc (c) String pattern matching such as like, starting with, ending with, etc.

- Select

>>> emp_df.select('name').show()
+----+
|name|
+----+
| San|
|Jade|
| Kun|
+----+

>>> emp_df.select(emp_df['age']).show()
+---+
|age|
+---+
| 32|
| 31|
| 30|
+---+

>>> emp_df.select('name', (emp_df['age'] * 1.25)).show()
+----+------------+
|name|(age * 1.25)|
+----+------------+
| San|        40.0|
|Jade|       38.75|
| Kun|        37.5|
+----+------------+

>>> emp_df.select('name', (emp_df['age'] * 1.25).alias('newage')).show()
+----+------+
|name|newage|
+----+------+
| San|  40.0|
|Jade| 38.75|
| Kun|  37.5|
+----+------+

>>> from pyspark.sql.functions import length
>>> emp_df.select(length(emp_df['name'])).show()
+------------+
|length(name)|
+------------+
|           3|
|           4|
|           3|
+------------+

>>> emp_df.selectExpr("name", "age * 1.25 as newage").show()
+----+------+
|name|newage|
+----+------+
| San| 40.00|
|Jade| 38.75|
| Kun| 37.50|
+----+------+


- Filter

>>> emp_df.filter(emp_df['salary'] > 5000).show()
+----+---+------+
|name|age|salary|
+----+---+------+
|Jade| 31|  5500|
| Kun| 30|  6000|
+----+---+------+

>>> emp_df.where(emp_df['salary'] > 5000).show()
+----+---+------+
|name|age|salary|
+----+---+------+
|Jade| 31|  5500|
| Kun| 30|  6000|
+----+---+------+

>>> emp_df.where(emp_df['salary'] == 6000).show()
+----+---+------+
|name|age|salary|
+----+---+------+
| Kun| 30|  6000|
+----+---+------+

>>> emp_df.where(emp_df['salary'] != 6000).show()
+----+---+------+
|name|age|salary|
+----+---+------+
| San| 32|  5000|
|Jade| 31|  5500|
+----+---+------+


- Distinct

>>> emp_df.filter(length(emp_df['name']) < 4 ).select(length(emp_df['name'])).show()
+------------+
|length(name)|
+------------+
|           3|
|           3|
+------------+

>>> emp_df.filter(length(emp_df['name']) < 4 ).select(length(emp_df['name'])).distinct().show()
+------------+
|length(name)|
+------------+
|           3|
+------------+


- Sort & Order By

>>> emp_df.sort('salary').show()
+----+---+------+
|name|age|salary|
+----+---+------+
| San| 32|  5000|
|Jade| 31|  5500|
| Kun| 30|  6000|
+----+---+------+

>>> emp_df.orderBy('age', 'salary').show()
+----+---+------+
|name|age|salary|
+----+---+------+
| Kun| 30|  6000|
|Jade| 31|  5500|
| San| 32|  5000|
+----+---+------+


- Limiting

>>> emp_df.orderBy('age', 'salary').limit(1).show()
+----+---+------+
|name|age|salary|
+----+---+------+
| Kun| 30|  6000|
+----+---+------+


- Union

>>> new_emp = spark.createDataFrame([('xun', 30, 7000)], schma_emp)
>>> new_emp.show()
+----+---+------+
|name|age|salary|
+----+---+------+
| xun| 30|  7000|
+----+---+------+

>>> emp_df.union(new_emp).show()
+----+---+------+
|name|age|salary|
+----+---+------+
| San| 32|  5000|
|Jade| 31|  5500|
| Kun| 30|  6000|
| xun| 30|  7000|
+----+---+------+

- Add Column

>>> emp_df.withColumn('cars', (emp_df['age'] - emp_df['age']) + random.randint(2,6)).show()
+----+---+------+----+
|name|age|salary|cars|
+----+---+------+----+
| San| 32|  5000|   4|
|Jade| 31|  5500|   4|
| Kun| 30|  6000|   4|
+----+---+------+----+


- Rename Column

>>> emp_df.withColumnRenamed('salary', 'pay').show()
+----+---+----+
|name|age| pay|
+----+---+----+
| San| 32|5000|
|Jade| 31|5500|
| Kun| 30|6000|
+----+---+----+


- Drop Column

>>> emp_df.drop('age').show()
+----+------+
|name|salary|
+----+------+
| San|  5000|
|Jade|  5500|
| Kun|  6000|
+----+------+


- Sample

>>> emp_df.sample(True, 0.6).show()
+----+---+------+
|name|age|salary|
+----+---+------+
| San| 32|  5000|
+----+---+------+

> To make sampling predictable we add seed

>>> emp_df.sample(True, 0.6, 1).show()
+----+---+------+
|name|age|salary|
+----+---+------+
| San| 32|  5000|
| Kun| 30|  6000|
+----+---+------+

>>> emp_df.sample(True, 0.6, 1).show()
+----+---+------+
|name|age|salary|
+----+---+------+
| San| 32|  5000|
| Kun| 30|  6000|
+----+---+------+


- Split Dataframe

>>> df1, df2 = emp_df.randomSplit([0.2, 0.8])
>>> df1.show()
+----+---+------+
|name|age|salary|
+----+---+------+
|Jade| 31|  5500|
| Kun| 30|  6000|
+----+---+------+

>>> df2.show()
+----+---+------+
|name|age|salary|
+----+---+------+
| San| 32|  5000|
+----+---+------+


### 3. DataFrames Handling Invalid Data


- Drop if any column invalid

>>> final_df.show()
+----+----+------+
|name| age|salary|
+----+----+------+
| San|  32|  5000|
|Jade|  31|  5500|
| Kun|  30|  6000|
| xun|  30|  null|
|null|null|  null|
+----+----+------+

>>> final_df.na.drop('any').show()
+----+---+------+
|name|age|salary|
+----+---+------+
| San| 32|  5000|
|Jade| 31|  5500|
| Kun| 30|  6000|
+----+---+------+


- Drop if All column invalid

>>> final_df.na.drop('all').show()
+----+---+------+
|name|age|salary|
+----+---+------+
| San| 32|  5000|
|Jade| 31|  5500|
| Kun| 30|  6000|
| xun| 30|  null|
+----+---+------+

- Select Column to check

>>> final_df.na.drop('any',subset=['name', 'age']).show()
+----+---+------+
|name|age|salary|
+----+---+------+
| San| 32|  5000|
|Jade| 31|  5500|
| Kun| 30|  6000|
| xun| 30|  null|
+----+---+------+

- Describe

>>> final_df.describe('age').show()
+-------+------------------+
|summary|               age|
+-------+------------------+
|  count|                 4|
|   mean|             30.75|
| stddev|0.9574271077563381|
|    min|                30|
|    max|                32|
+-------+------------------+

>>> final_df.count()
5


### 3. DataFrames Action

- Show

>>> emp_df.show(1, truncate=False)
+---+----+------+
|age|name|salary|
+---+----+------+
|32 |San |5000  |
+---+----+------+
only showing top 1 row

> Truncate avoid column truncation while showing

- Head/First/Take

>>> emp_df.first()
Row(age=32, name='San', salary=5000)
>>> emp_df.head(1)
[Row(age=32, name='San', salary=5000)]
>>> emp_df.take(1)
[Row(age=32, name='San', salary=5000)]

- Collect

>>> emp_df.collect()
[Row(age=32, name='San', salary=5000), Row(age=31, name='Jade', salary=5500), Row(age=30, name='Kun', salary=6000)]

- Count

>>> emp_df.count()
3

### 4. SQL

> Before you can issue SQL queries to manipulate them, you need to register them as temporary views. Each view has a name, and that is what is used as the table name in the select clause. Spark provides two levels of scoping for the temporary views. 
> One is at the Spark session level. When a DataFrame is registered at this level, only the queries that are issued in the same session can refer to that DataFrame. The session-scoped level will disappear when a Spark session is closed. 
> The second scoping level is at the global level, which means these views are available to SQL statements in all Spark sessions. All the registered views are maintained in the Spark metadata catalog that can be accessed through SparkSession.

- List metadata catalog

>>> spark.catalog.listTables()
[]

>>> emp_df.createOrReplaceTempView('emp')

>>> spark.catalog.listTables()
20/02/17 23:46:25 WARN ObjectStore: Failed to get database global_temp, returning NoSuchObjectException
[Table(name='emp', database=None, description=None, tableType='TEMPORARY', isTemporary=True)]

>>> spark.catalog.listDatabases()
[Database(name='default', description='Default Hive database', locationUri='file:/Users/apple/TEST/spark/spark-kubernetes-master/BUILD_BINARY/spark/spark2/bin/spark-warehouse')]

>>> emp_df.createOrReplaceGlobalTempView('empg')

- Query

>>> spark.sql('select * from emp').show()
+---+----+------+
|age|name|salary|
+---+----+------+
| 32| San|  5000|
| 31|Jade|  5500|
| 30| Kun|  6000|
+---+----+------+

>>> spark.sql("select * from emp where age > 30").show()
+---+----+------+
|age|name|salary|
+---+----+------+
| 32| San|  5000|
| 31|Jade|  5500|
+---+----+------+

>>> spark.sql("select * from emp where name like 'S%'").show()
+---+----+------+
|age|name|salary|
+---+----+------+
| 32| San|  5000|
+---+----+------+

>>> spark.sql("select * from emp where age > 30").orderBy('age').show()
+---+----+------+
|age|name|salary|
+---+----+------+
| 31|Jade|  5500|
| 32| San|  5000|
+---+----+------+

>>> spark.sql("select * from emp where age > 30").where(emp_df['salary'] > 5100).show()
+---+----+------+
|age|name|salary|
+---+----+------+
| 31|Jade|  5500|
+---+----+------+


>>> spark.sql("select * from (select name, age from emp) where age > 31").show()
+----+---+
|name|age|
+----+---+
| San| 32|
+----+---+

>>> spark.sql("select * from emp where name in (select name from emp where age > 31)").show()
+---+----+------+
|age|name|salary|
+---+----+------+
| 32| San|  5000|
+---+----+------+


>>> spark.sql("select count(*) from global_temp.empg").show()
+--------+
|count(1)|
+--------+
|       3|
+--------+

> Note to access Global view you need to prefix viewname with global_temp


>>> spark.sql("select * from parquet.`/Users/apple/TEST/pyspark/emp.parquet`").show()
+----+---+------+                                                               
|name|age|salary|
+----+---+------+
| San| 32|  5000|
|Jade| 31|  5500|
| Kun| 30|  6000|
+----+---+------+

> Note above we read data directly from Parquet file


### 5. Saving Sttructured Data

> DataFrame.write.format(...).mode(...).option(...).partitionBy(...).bucketBy(...) .sortBy(...).save(path)

> Default file format is Parquet

> If the output directory exist you can choose one mode append, overwrite, error|errorIfExists|default, ignore to control behaviour.

>>> emp_df.write.format('csv').mode('append').option('sep', '|').save('/Users/apple/TEST/pyspark/output1')
>>>

apples-MacBook-Air:pyspark apple$ cat output1/part-00000-8abdb2ed-52bd-4461-a93a-9db6bd247d21-c000.csv 
32|San|5000
31|Jade|5500
30|Kun|6000

>>> new_emp = spark.createDataFrame([('xun', 30, 7000)], schma_emp)
>>> emp_df = emp_df.union(new_emp)
>>> emp_df.show()
+---+----+------+                                                               
|age|name|salary|
+---+----+------+
| 32| San|  5000|
| 31|Jade|  5500|
| 30| Kun|  6000|
|xun|  30|  7000|
+---+----+------+

>>> schma_emp = StructType([StructField('name', StringType()), StructField('age', LongType()), StructField('salary', LongType())])

apples-MacBook-Air:pyspark apple$ ls -ltr output1/
total 24
-rw-r--r--  1 apple  staff  37 Feb 18 00:25 part-00000-8abdb2ed-52bd-4461-a93a-9db6bd247d21-c000.csv
-rw-r--r--  1 apple  staff  37 Feb 18 00:31 part-00000-ab546110-d3aa-4a1d-a08a-dac1a8ba9115-c000.csv
-rw-r--r--  1 apple  staff  12 Feb 18 00:31 part-00004-ab546110-d3aa-4a1d-a08a-dac1a8ba9115-c000.csv
-rw-r--r--  1 apple  staff   0 Feb 18 00:31 _SUCCESS
apples-MacBook-Air:pyspark apple$ 
apples-MacBook-Air:pyspark apple$ cat output1/*.csv
32|San|5000
31|Jade|5500
30|Kun|6000
32|San|5000
31|Jade|5500
30|Kun|6000
xun|30|7000

- Number of Partition

>>> emp_df.rdd.getNumPartitions()
5

>>> emp_df = emp_df.coalesce(1)

>>> emp_df.rdd.getNumPartitions()
1


- Partition

>>> emp_df.write.partitionBy('age').save('/Users/apple/TEST/pyspark/output2')
>>>

apples-MacBook-Air:pyspark apple$ ls -ltr output2/
total 0
drwxr-xr-x  4 apple  staff  128 Feb 18 00:38 age=30
drwxr-xr-x  4 apple  staff  128 Feb 18 00:38 age=31
drwxr-xr-x  4 apple  staff  128 Feb 18 00:38 age=32
-rw-r--r--  1 apple  staff    0 Feb 18 00:38 _SUCCESS


- Persistane

>>> emp_df.createOrReplaceTempView('emp')
>>> spark.catalog.cacheTable('emp')
>>> emp_df.count()
4

> Note we trigger the action count so that the Persistance operation cacheTable materialises.


### 6. Aggregation

>>> df1 = spark.createDataFrame([('san', 32, 5000), ('kun', 33, 5500), ('jade', 34, 5500), ('xun', 31, 6000)], ['name', 'age', 'pay'])
>>> df1.show()
+----+---+----+                                                                 
|name|age| pay|
+----+---+----+
| san| 32|5000|
| kun| 33|5500|
|jade| 34|5500|
| xun| 31|6000|
+----+---+----+

- Count

>>> df1.count()
4

>>> df1.select('pay').count()
4

>>> df1.createOrReplaceTempView('df1')
>>> spark.sql('select count(pay) from df1').show()
+----------+
|count(pay)|
+----------+
|         4|
+----------+

>>> df2 = spark.createDataFrame([('geome', 35, 3500), ('slim', 31, val)], ['name', 'age', 'pay'])
>>> df1 = df1.union(df2)
>>> df1.show()
+-----+---+----+
| name|age| pay|
+-----+---+----+
|  san| 32|5000|
|  kun| 33|5500|
| jade| 34|5500|
|  xun| 31|6000|
|geome| 35|3500|
| slim| 31|null|
+-----+---+----+

>>> df1.count()
6

>>> df1.createOrReplaceTempView('df1')
>>> spark.sql('select count(pay) from df1').show()
+----------+
|count(pay)|
+----------+
|         5|
+----------+

>>> from pyspark.sql.functions import count
>>> df1.select(count(df1.pay)).show()
+----------+
|count(pay)|
+----------+
|         5|
+----------+

> Count doesnot count Null values.

- Count Distinct

>>> from pyspark.sql.functions import countDistinct
>>> df1.select(countDistinct(df1.pay)).show()
+-------------------+                                                           
|count(DISTINCT pay)|
+-------------------+
|                  4|
+-------------------+

>>> spark.sql('select count(pay) from (select distinct pay from df1)').show()
+----------+                                                                    
|count(pay)|
+----------+
|         4|
+----------+


- Approx distinct count

>>> from pyspark.sql.functions import approx_count_distinct
>>> df1.select(approx_count_distinct(df1.pay, 0.05)).show()
+--------------------------+
|approx_count_distinct(pay)|
+--------------------------+
|                         4|
+--------------------------+


- Sum, Min, Max

>>> from pyspark.sql.functions import sum, max, min, sumDistinct
>>> df1.select(sum(df1.pay), sumDistinct(df1.pay), max(df1.pay), min(df1.pay)).show()                                        
+--------+-----------------+--------+--------+                                  
|sum(pay)|sum(DISTINCT pay)|max(pay)|min(pay)|
+--------+-----------------+--------+--------+
|   25500|            20000|    6000|    3500|
+--------+-----------------+--------+--------+


- Statistical averages

>>> from pyspark.sql.functions import avg, skewness, kurtosis, variance, stddev
>>> df1.select(avg(df1.pay), skewness(df1.pay), kurtosis(df1.pay), variance(df1.pay), stddev(df1.pay)).show()
+--------+------------------+-------------------+-------------+-----------------+
|avg(pay)|     skewness(pay)|      kurtosis(pay)|var_samp(pay)| stddev_samp(pay)|
+--------+------------------+-------------------+-------------+-----------------+
|  5100.0|-1.017952296026958|-0.3480642804967129|     925000.0|961.7692030835673|
+--------+------------------+-------------------+-------------+-----------------+


- Group aggreegation

>>> df1.groupBy('age').count().show()
+---+-----+
|age|count|
+---+-----+
| 34|    1|
| 32|    1|
| 31|    2|
| 33|    1|
| 35|    1|
+---+-----+


>>> df1.groupBy('age').count().where(df1['age'] < 33).orderBy('age').show()
+---+-----+                                                                     
|age|count|
+---+-----+
| 31|    2|
| 32|    1|
+---+-----+

>>> df1.groupBy('age').max('pay').where(df1['age'] == 31).show()
+---+--------+
|age|max(pay)|
+---+--------+
| 31|    6000|
+---+--------+

>>> df1.groupBy('age').min('pay').where(df1['age'] == 31).show()
+---+--------+
|age|min(pay)|
+---+--------+
| 31|    6000|
+---+--------+

>>> df1.groupBy('age').avg('pay').where(df1['age'] == 31).show()
+---+--------+
|age|avg(pay)|
+---+--------+
| 31|  6000.0|
+---+--------+

>>> df1.groupBy('age').sum('pay').where(df1['age'] == 31).show()
+---+--------+
|age|sum(pay)|
+---+--------+
| 31|    6000|
+---+--------+

>>> df1.groupBy('age').agg(max('pay'), sum('pay')).where(df1['age'] == 31).show()
+---+--------+--------+
|age|max(pay)|sum(pay)|
+---+--------+--------+
| 31|    6000|    6000|
+---+--------+--------+


>>> from pyspark.sql.functions import collect_list, collect_set
>>> df1.groupBy('age').agg(collect_list('name'), collect_set('name')).show()
+---+------------------+-----------------+
|age|collect_list(name)|collect_set(name)|
+---+------------------+-----------------+
| 34|            [jade]|           [jade]|
| 32|             [san]|            [san]|
| 31|       [xun, slim]|      [slim, xun]|
| 33|             [kun]|            [kun]|
| 35|           [geome]|          [geome]|
+---+------------------+-----------------+



- Average over Pivoting

>>> df1.groupBy('age').pivot('name').avg('pay').show()
+---+------+------+------+------+----+------+                                   
|age| geome|  jade|   kun|   san|slim|   xun|
+---+------+------+------+------+----+------+
| 34|  null|5500.0|  null|  null|null|  null|
| 32|  null|  null|  null|5000.0|null|  null|
| 31|  null|  null|  null|  null|null|6000.0|
| 33|  null|  null|5500.0|  null|null|  null|
| 35|3500.0|  null|  null|  null|null|  null|
+---+------+------+------+------+----+------+


### 7. Join

>>> df1 = spark.createDataFrame([('san', 32, 5000), ('kun', 33, 5500), ('jade', 34, 5500), ('xun', 31, 6000)], ['name', 'age', 'pay'])
>>> df1.show()
+----+---+----+                                                                 
|name|age| pay|
+----+---+----+
| san| 32|5000|
| kun| 33|5500|
|jade| 34|5500|
| xun| 31|6000|
+----+---+----+

>>> df2 = spark.createDataFrame([('san', 'kolkata'), ('jade', 'berut'), ('jewel', 'delhi')], ['name', 'city'])
>>> df2.show()
+-----+-------+
| name|   city|
+-----+-------+
|  san|kolkata|
| jade|  berut|
|jewel|  delhi|
+-----+-------+


- Inner

>>> df1.join(df2, df1['name'] == df2['name'], 'inner').show()
+----+---+----+----+-------+
|name|age| pay|name|   city|
+----+---+----+----+-------+
| san| 32|5000| san|kolkata|
|jade| 34|5500|jade|  berut|
+----+---+----+----+-------+


>>> df1.createOrReplaceTempView('emppay')
>>> df2.createOrReplaceTempView('empadd')
>>> 
>>> spark.sql('select * from emppay join empadd on emppay.name == empadd.name').show()
+----+---+----+----+-------+
|name|age| pay|name|   city|
+----+---+----+----+-------+
| san| 32|5000| san|kolkata|
|jade| 34|5500|jade|  berut|
+----+---+----+----+-------+


- Left Outer

>>> df1.join(df2, df1['name'] == df2['name'], 'left_outer').show()
+----+---+----+----+-------+
|name|age| pay|name|   city|
+----+---+----+----+-------+
| san| 32|5000| san|kolkata|
| kun| 33|5500|null|   null|
| xun| 31|6000|null|   null|
|jade| 34|5500|jade|  berut|
+----+---+----+----+-------+

>>> spark.sql('select * from emppay left outer join empadd on emppay.name == empadd.name').show()
+----+---+----+----+-------+
|name|age| pay|name|   city|
+----+---+----+----+-------+
| san| 32|5000| san|kolkata|
| kun| 33|5500|null|   null|
| xun| 31|6000|null|   null|
|jade| 34|5500|jade|  berut|
+----+---+----+----+-------+


- Right Outer

>>> df1.join(df2, df1['name'] == df2['name'], 'right_outer').show()
+----+----+----+-----+-------+
|name| age| pay| name|   city|
+----+----+----+-----+-------+
| san|  32|5000|  san|kolkata|
|null|null|null|jewel|  delhi|
|jade|  34|5500| jade|  berut|
+----+----+----+-----+-------+

>>> spark.sql('select * from emppay right outer join empadd on emppay.name == empadd.name').show()
+----+----+----+-----+-------+
|name| age| pay| name|   city|
+----+----+----+-----+-------+
| san|  32|5000|  san|kolkata|
|null|null|null|jewel|  delhi|
|jade|  34|5500| jade|  berut|
+----+----+----+-----+-------+

- Full Outer

>>> df1.join(df2, df1['name'] == df2['name'], 'outer').show()
+----+----+----+-----+-------+
|name| age| pay| name|   city|
+----+----+----+-----+-------+
| san|  32|5000|  san|kolkata|
|null|null|null|jewel|  delhi|
| kun|  33|5500| null|   null|
| xun|  31|6000| null|   null|
|jade|  34|5500| jade|  berut|
+----+----+----+-----+-------+

>>> spark.sql('select * from emppay full outer join empadd on emppay.name == empadd.name').show()
+----+----+----+-----+-------+
|name| age| pay| name|   city|
+----+----+----+-----+-------+
| san|  32|5000|  san|kolkata|
|null|null|null|jewel|  delhi|
| kun|  33|5500| null|   null|
| xun|  31|6000| null|   null|
|jade|  34|5500| jade|  berut|
+----+----+----+-----+-------+



- Anti and Semi

>>> df1.join(df2, df1['name'] == df2['name'], 'left_anti').show()
+----+---+----+
|name|age| pay|
+----+---+----+
| kun| 33|5500|
| xun| 31|6000|
+----+---+----+

>>> df1.join(df2, df1['name'] == df2['name'], 'left_semi').show()
+----+---+----+
|name|age| pay|
+----+---+----+
| san| 32|5000|
|jade| 34|5500|
+----+---+----+


- Cross Join

>>> df1.crossJoin(df2).show()
+----+---+----+-----+-------+
|name|age| pay| name|   city|
+----+---+----+-----+-------+
| san| 32|5000|  san|kolkata|
| san| 32|5000| jade|  berut|
| san| 32|5000|jewel|  delhi|
| kun| 33|5500|  san|kolkata|
| kun| 33|5500| jade|  berut|
| kun| 33|5500|jewel|  delhi|
|jade| 34|5500|  san|kolkata|
|jade| 34|5500| jade|  berut|
|jade| 34|5500|jewel|  delhi|
| xun| 31|6000|  san|kolkata|
| xun| 31|6000| jade|  berut|
| xun| 31|6000|jewel|  delhi|
+----+---+----+-----+-------+


- oulum Operation


>>> df3 = df2.withColumn('workplace', df2['city'])
>>> df3.show()
+-----+-------+---------+
| name|   city|workplace|
+-----+-------+---------+
|  san|kolkata|  kolkata|
| jade|  berut|    berut|
|jewel|  delhi|    delhi|
+-----+-------+---------+


### 8. Join Strategy

- Suffle Hash Join

	1. Compute hash of each row of both dataset
	2. Perform modulo of each hash with target partition number to choose partition placement for both dataset

- Broadcast Hash Join

	1. Applicable to case where one data set is sufficiently small
	2. Broadcast the entire small dataset to every partition of larger data set


Spark automatically choose join type but hint can be provided


>>> from pyspark.sql.functions import broadcast
>>> df1.join(broadcast(df2), df1['name'] == df2['name'], 'inner').show()
+----+---+----+----+-------+
|name|age| pay|name|   city|
+----+---+----+----+-------+
| san| 32|5000| san|kolkata|
|jade| 34|5500|jade|  berut|
+----+---+----+----+-------+

>>> df1.join(broadcast(df2), df1['name'] == df2['name'], 'inner').explain()
== Physical Plan ==
*(2) BroadcastHashJoin [name#0], [name#16], Inner, BuildRight
:- *(2) Filter isnotnull(name#0)
:  +- Scan ExistingRDD[name#0,age#1L,pay#2L]
+- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]))
   +- *(1) Filter isnotnull(name#16)
      +- Scan ExistingRDD[name#16,city#17]


>>> spark.sql('select /*+ mapjoin(empadd) */ * from emppay join empadd on emppay.name == empadd.name').explain()
== Physical Plan ==
*(2) BroadcastHashJoin [name#0], [name#16], Inner, BuildRight
:- *(2) Filter isnotnull(name#0)
:  +- Scan ExistingRDD[name#0,age#1L,pay#2L]
+- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]))
   +- *(1) Filter isnotnull(name#16)
      +- Scan ExistingRDD[name#16,city#17]



### 9. Buildin Functions

- Take one or more column of one row and result in new column

- Date

>>> from pyspark.sql.functions import to_date
>>> 
>>> df4 = spark.createDataFrame([('jade', '1985-04-12'), ('kun', '1986-05-30')], ['name', 'dob'])
>>> df4.select('name', to_date('dob').alias('date of birth')).show()
+----+-------------+
|name|date of birth|
+----+-------------+
|jade|   1985-04-12|
| kun|   1986-05-30|
+----+-------------+


>>> from pyspark.sql.functions import date_format
>>> df4.select('name', date_format('dob', 'YYYY-dd-MM')).show()
+----+----------------------------+
|name|date_format(dob, YYYY-dd-MM)|
+----+----------------------------+
|jade|                  1985-12-04|
| kun|                  1986-30-05|
+----+----------------------------+


>>> from pyspark.sql.functions import date_add
>>> df4.select('name', date_add('dob', 15)).show()
+----+-----------------+
|name|date_add(dob, 15)|
+----+-----------------+
|jade|       1985-04-27|
| kun|       1986-06-14|
+----+-----------------+


- String

from pyspark.sql.functions import concat_ws, lower, concat, upper, translate

>>> df4.select(concat_ws('  ','name', 'dob').alias('empname')).select('empname').show()
+----------------+
|         empname|
+----------------+
|jade  1985-04-12|
| kun  1986-05-30|
+----------------+


>>> df4.select(upper(df4['name'])).show()
+-----------+
|upper(name)|
+-----------+
|       JADE|
|        KUN|
+-----------+

>>> df4.select(translate('name', 'k', 'x'), 'name').show()
+---------------------+----+
|translate(name, k, x)|name|
+---------------------+----+
|                 jade|jade|
|                  xun| kun|
+---------------------+----+


- Math 

from pyspark.sql.functions import round

>>> df1.select( round(df1['pay']/df1['age']).alias('parbyage') ).show()
+--------+
|parbyage|
+--------+
|   156.0|
|   167.0|
|   162.0|
|   194.0|
+--------+

- Mislenious

>>> from pyspark.sql.functions import monotonically_increasing_id, when


>>> df1.select(monotonically_increasing_id().alias('id'), 'name').show()
+-----------+----+
|         id|name|
+-----------+----+
|          0| san|
| 8589934592| kun|
|17179869184|jade|
|25769803776| xun|
+-----------+----+


>>> df2.select('name', when(df2['city'] == 'kolkata', 'indian').otherwise('nonindian').alias('country')).show()
+-----+---------+
| name|  country|
+-----+---------+
|  san|   indian|
| jade|nonindian|
|jewel|nonindian|
+-----+---------+


### 10. User Defined Function (UDF)

>>> from pyspark.sql.types import StringType
>>> from pyspark.sql.functions import udf


>>> df0 = spark.createDataFrame([(1,), (2,), (3,)], ['val'])
>>> df0.show()
+---+                                                                           
|val|
+---+
|  1|
|  2|
|  3|
+---+

>>> def even_odd(num) :
...   if num % 2 :
...      return 'odd'
...   else:
...      return 'even'
... 

>>> eod = udf(lambda x: even_odd(x), StringType())

>>> df0.select('val', eod('val').alias('type')).show()
+---+----+
|val|type|
+---+----+
|  1| odd|
|  2|even|
|  3| odd|
+---+----+


>>> eod = spark.udf.register('evenodd', even_odd, StringType())
>>> df0.select('val', eod('val')).show()
+---+------------+
|val|evenodd(val)|
+---+------------+
|  1|         odd|
|  2|        even|
|  3|         odd|
+---+------------+


### 11. Cube & Rollup

>>> from pyspark.sql.functions import sum

df1 = spark.createDataFrame([('san', 32, 'india'), ('kun', 33, 'china'), ('jade', 34, 'india'), ('xun', 31, 'china')], ['name', 'age', 'country'])


- Rollup

>>> df1.rollup('country').agg(sum('age')).show()
+-------+--------+
|country|sum(age)|
+-------+--------+
|   null|     130|
|  china|      64|
|  india|      66|
+-------+--------+


df2 = spark.createDataFrame([('san', 'kolkata', 32, 'india'), ('kun', 'tiangen', 33, 'china'), ('jade', 'kolkata', 34, 'india'), ('xun', 'senzen', 31, 'china')], ['name', 'city', 'age', 'country'])

>>> df2.rollup('city', 'country').agg(sum('age')).show()
+-------+-------+--------+
|   city|country|sum(age)|
+-------+-------+--------+
|tiangen|  china|      33|
|tiangen|   null|      33|
| senzen|   null|      31|
|   null|   null|     130|
| senzen|  china|      31|
|kolkata|  india|      66|
|kolkata|   null|      66|
+-------+-------+--------+

>>> df2.rollup('city', 'country').agg(sum('age')).orderBy('city').show()
+-------+-------+--------+                                                      
|   city|country|sum(age)|
+-------+-------+--------+
|   null|   null|     130|
|kolkata|  india|      66|
|kolkata|   null|      66|
| senzen|  china|      31|
| senzen|   null|      31|
|tiangen|   null|      33|
|tiangen|  china|      33|
+-------+-------+--------+

>>> df2.rollup('city', 'country').agg(sum('age')).orderBy('city', 'country').show()
+-------+-------+--------+
|   city|country|sum(age)|
+-------+-------+--------+
|   null|   null|     130|
|kolkata|   null|      66|
|kolkata|  india|      66|
| senzen|   null|      31|
| senzen|  china|      31|
|tiangen|   null|      33|
|tiangen|  china|      33|
+-------+-------+--------+


- Cube (Special case of Rollup where aggregation is done with all possible column combination)

>>> df2.cube('city', 'country').agg(sum('age')).orderBy('city', 'country').show()
+-------+-------+--------+                                                      
|   city|country|sum(age)|
+-------+-------+--------+
|   null|   null|     130|
|   null|  china|      64|
|   null|  india|      66|
|kolkata|   null|      66|
|kolkata|  india|      66|
| senzen|   null|      31|
| senzen|  china|      31|
|tiangen|   null|      33|
|tiangen|  china|      33|
+-------+-------+--------+


### 12. Windowing

https://www.kaggle.com/sudalairajkumar/novel-corona-virus-2019-dataset/version/20

>>> cdf = spark.read.option('header','true').csv('2019_nCoV_data.csv')
>>> cdf.printSchema()
root
 |-- sno: string (nullable = true)
 |-- date: string (nullable = true)
 |-- city: string (nullable = true)
 |-- country: string (nullable = true)
 |-- last_update: string (nullable = true)
 |-- confirmed: string (nullable = true)
 |-- deaths: string (nullable = true)
 |-- recovered: string (nullable = true)

>>> cdfclean = cdf.select(cdf['sno'].cast('int'), to_timestamp(cdf['date'], 'MM/dd/yyyy HH:mm:ss').alias('date'), 'city', 'country', to_timestamp(cdf['last_update'], 'MM/dd/yyyy HH:mm:ss').alias('last_update'), cdf['confirmed'].cast('int'), cdf['deaths'].cast('int'), cdf['recovered'].cast('int'))
>>> cdfclean.printSchema()                                                                                                  root
 |-- sno: integer (nullable = true)
 |-- date: timestamp (nullable = true)
 |-- city: string (nullable = true)
 |-- country: string (nullable = true)
 |-- last_update: timestamp (nullable = true)
 |-- confirmed: integer (nullable = true)
 |-- deaths: integer (nullable = true)
 |-- recovered: integer (nullable = true)

>>> cdfclean.show(5)
+---+-------------------+---------+-------+-------------------+---------+------+---------+
|sno|               date|     city|country|        last_update|confirmed|deaths|recovered|
+---+-------------------+---------+-------+-------------------+---------+------+---------+
|  1|2020-01-22 12:00:00|    Anhui|  China|2020-01-22 12:00:00|        1|     0|        0|
|  2|2020-01-22 12:00:00|  Beijing|  China|2020-01-22 12:00:00|       14|     0|        0|
|  3|2020-01-22 12:00:00|Chongqing|  China|2020-01-22 12:00:00|        6|     0|        0|
|  4|2020-01-22 12:00:00|   Fujian|  China|2020-01-22 12:00:00|        1|     0|        0|
|  5|2020-01-22 12:00:00|    Gansu|  China|2020-01-22 12:00:00|        0|     0|        0|
+---+-------------------+---------+-------+-------------------+---------+------+---------+



- Weekwise windows (Tumbling Window)

>>> cdfclean.groupBy(window('date', '1 week')).avg('confirmed').show(truncate=False)
+------------------------------------------+------------------+
|window                                    |avg(confirmed)    |
+------------------------------------------+------------------+
|[2020-01-16 01:00:00, 2020-01-23 01:00:00]|14.605263157894736|
|[2020-02-06 01:00:00, 2020-02-13 01:00:00]|571.5544554455446 |
|[2020-02-13 01:00:00, 2020-02-20 01:00:00]|921.9973262032086 |
|[2020-01-23 01:00:00, 2020-01-30 01:00:00]|73.35311572700297 |
|[2020-01-30 01:00:00, 2020-02-06 01:00:00]|263.9032258064516 |
+------------------------------------------+------------------+

>>> cdfclean.groupBy(window('date', '1 week')).agg(avg('confirmed').alias('weekly_case')).orderBy('weekly_case').show(truncate=False)
+------------------------------------------+------------------+
|window                                    |weekly_case       |
+------------------------------------------+------------------+
|[2020-01-16 01:00:00, 2020-01-23 01:00:00]|14.605263157894736|
|[2020-01-23 01:00:00, 2020-01-30 01:00:00]|73.35311572700297 |
|[2020-01-30 01:00:00, 2020-02-06 01:00:00]|263.9032258064516 |
|[2020-02-06 01:00:00, 2020-02-13 01:00:00]|571.5544554455446 |
|[2020-02-13 01:00:00, 2020-02-20 01:00:00]|921.9973262032086 |
+------------------------------------------+------------------+

>>> cdfclean.groupBy(window('date', '1 week')).agg(avg('confirmed').alias('weekly_case')).orderBy('weekly_case').printSchema()
root
 |-- window: struct (nullable = false)
 |    |-- start: timestamp (nullable = true)
 |    |-- end: timestamp (nullable = true)
 |-- weekly_case: double (nullable = true)


>>> cdfclean.groupBy(window('date', '1 week')).agg(avg('confirmed').alias('weekly_case')).orderBy('weekly_case').select('window.start', 'window.end', round('weekly_case').alias('cases')).show()
+-------------------+-------------------+-----+
|              start|                end|cases|
+-------------------+-------------------+-----+
|2020-01-16 01:00:00|2020-01-23 01:00:00| 15.0|
|2020-01-23 01:00:00|2020-01-30 01:00:00| 73.0|
|2020-01-30 01:00:00|2020-02-06 01:00:00|264.0|
|2020-02-06 01:00:00|2020-02-13 01:00:00|572.0|
|2020-02-13 01:00:00|2020-02-20 01:00:00|922.0|
+-------------------+-------------------+-----+


- 2 Weekly Average Every 3 Days (Sliding Window)

>>> cdfclean.groupBy(window('date', '2 week', '3 day')).agg(avg('confirmed').alias('weekly_case')).orderBy('weekly_case').select('window.start', 'window.end', round('weekly_case').alias('cases')).show()
+-------------------+-------------------+-----+
|              start|                end|cases|
+-------------------+-------------------+-----+
|2020-01-09 01:00:00|2020-01-23 01:00:00| 15.0|
|2020-01-12 01:00:00|2020-01-26 01:00:00| 25.0|
|2020-01-15 01:00:00|2020-01-29 01:00:00| 55.0|
|2020-01-18 01:00:00|2020-02-01 01:00:00| 93.0|
|2020-01-21 01:00:00|2020-02-04 01:00:00|141.0|
|2020-01-24 01:00:00|2020-02-07 01:00:00|214.0|
|2020-01-27 01:00:00|2020-02-10 01:00:00|309.0|
|2020-01-30 01:00:00|2020-02-13 01:00:00|424.0|
|2020-02-02 01:00:00|2020-02-16 01:00:00|573.0|
|2020-02-05 01:00:00|2020-02-19 01:00:00|693.0|
|2020-02-08 01:00:00|2020-02-22 01:00:00|776.0|
|2020-02-11 01:00:00|2020-02-25 01:00:00|866.0|
|2020-02-14 01:00:00|2020-02-28 01:00:00|935.0|
|2020-02-17 01:00:00|2020-03-02 01:00:00|977.0|
+-------------------+-------------------+-----+


- Custom windows

>>> from pyspark.sql import Window

- Moving average of 3 pay window per name

df1 = spark.createDataFrame([("John", "2017-07-06", 27.33),("John", "2017-07-04", 21.72),("Mary",  "2017-07-07", 69.74),("Mary",  "2017-07-01", 59.44),("Mary",  "2017-07-05", 80.14)], ['name', 'date', 'pay'])

>>> mov_avg = Window.partitionBy('name').orderBy('date').rowsBetween(Window.currentRow-1, Window.currentRow+1)

>>> df1.select('name', 'date', 'pay', round(avg('pay').over(mov_avg)).alias('moving_avg_pay')).show()
+----+----------+-----+--------------+
|name|      date|  pay|moving_avg_pay|
+----+----------+-----+--------------+
|Mary|2017-07-01|59.44|          70.0|
|Mary|2017-07-05|80.14|          70.0|
|Mary|2017-07-07|69.74|          75.0|
|John|2017-07-04|21.72|          25.0|
|John|2017-07-06|27.33|          25.0|
+----+----------+-----+--------------+


- Differnce of pay from max pay per name

>>> er = Window.partitionBy('name').orderBy('date').rangeBetween(Window.unboundedPreceding,Window.unboundedFollowing)
>>> amntdiff = max(df1['pay']).over(er) - df1['pay']
>>> df1.select('name', 'date', 'pay', round(amntdiff).alias('diff_2_max')).show()
+----+----------+-----+----------+
|name|      date|  pay|diff_2_max|
+----+----------+-----+----------+
|Mary|2017-07-01|59.44|      21.0|
|Mary|2017-07-05|80.14|       0.0|
|Mary|2017-07-07|69.74|      10.0|
|John|2017-07-04|21.72|       6.0|
|John|2017-07-06|27.33|       0.0|
+----+----------+-----+----------+


### 12. Optimisation

- Show physical plan

>>> df1.select('name', 'date', 'pay', round(amntdiff).alias('diff_2_max')).explain()
== Physical Plan ==
*(2) Project [name#1200, date#1201, pay#1202, round((_we0#2505 - pay#1202), 0) AS diff_2_max#2504]
+- Window [max(pay#1202) windowspecdefinition(name#1200, date#1201 ASC NULLS FIRST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), unboundedfollowing$())) AS _we0#2505], [name#1200], [date#1201 ASC NULLS FIRST]
   +- *(1) Sort [name#1200 ASC NULLS FIRST, date#1201 ASC NULLS FIRST], false, 0
      +- Exchange hashpartitioning(name#1200, 200)
         +- Scan ExistingRDD[name#1200,date#1201,pay#1202]


- Detailed Optimisation

>>> df1.select('name', 'date', 'pay', round(amntdiff).alias('diff_2_max')).explain(True)
== Parsed Logical Plan ==
'Project [unresolvedalias('name, None), unresolvedalias('date, None), unresolvedalias('pay, None), round((max(pay#1202) windowspecdefinition('name, 'date ASC NULLS FIRST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), unboundedfollowing$())) - pay#1202), 0) AS diff_2_max#2510]
+- LogicalRDD [name#1200, date#1201, pay#1202], false

== Analyzed Logical Plan ==
name: string, date: string, pay: double, diff_2_max: double
Project [name#1200, date#1201, pay#1202, diff_2_max#2510]
+- Project [name#1200, date#1201, pay#1202, _we0#2511, round((_we0#2511 - pay#1202), 0) AS diff_2_max#2510]
   +- Window [max(pay#1202) windowspecdefinition(name#1200, date#1201 ASC NULLS FIRST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), unboundedfollowing$())) AS _we0#2511], [name#1200], [date#1201 ASC NULLS FIRST]
      +- Project [name#1200, date#1201, pay#1202]
         +- LogicalRDD [name#1200, date#1201, pay#1202], false

== Optimized Logical Plan ==
Project [name#1200, date#1201, pay#1202, round((_we0#2511 - pay#1202), 0) AS diff_2_max#2510]
+- Window [max(pay#1202) windowspecdefinition(name#1200, date#1201 ASC NULLS FIRST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), unboundedfollowing$())) AS _we0#2511], [name#1200], [date#1201 ASC NULLS FIRST]
   +- LogicalRDD [name#1200, date#1201, pay#1202], false

== Physical Plan ==
*(2) Project [name#1200, date#1201, pay#1202, round((_we0#2511 - pay#1202), 0) AS diff_2_max#2510]
+- Window [max(pay#1202) windowspecdefinition(name#1200, date#1201 ASC NULLS FIRST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), unboundedfollowing$())) AS _we0#2511], [name#1200], [date#1201 ASC NULLS FIRST]
   +- *(1) Sort [name#1200 ASC NULLS FIRST, date#1201 ASC NULLS FIRST], false, 0
      +- Exchange hashpartitioning(name#1200, 200)
         +- Scan ExistingRDD[name#1200,date#1201,pay#1202]


