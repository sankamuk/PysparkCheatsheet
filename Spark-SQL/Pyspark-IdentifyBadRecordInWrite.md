# Identify Bad Record in Spark Writter

DataFrameWriter handles batch writes and most of the time a single record fails the whole Job and there is no way to identify the bad records which failed to get itself inserted into the external storage system.

## Setup

In this recipe i try to handle this problem. We create a Dataframe and try to insert into MySQL Table.

- Source Dataset:

| id | name |
|---|---|
|1|san|
|2|dab|
|3|ali|
|2|jew|
|5|abb|
|6|sah|
|7|ami|
|8|kan|
|9|abcdefghijkh|


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

> Note in our dataset we identify two issue
> 
>      Integrity constrain violation as primary key '2' repeated twice
>      
>      Name column in one row exceeds maximum length


## Lets see the solution

- Setup Spark Session and create dummy source data.

```python
import os
import math
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

split_sz = 5

os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /root/mysql-connector-java-8.0.30.jar pyspark-shell'

spark = SparkSession.builder.appName('Test Application').enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)

print("Creating source dataframe.")
df = spark.createDataFrame([(1, 'san'), (2, 'dab'), (3, 'ali'), (2, 'jew'), (5, 'abb'), (6, 'sah'), (7, 'ami'), (8, 'kan'), (9, 'abcdefghijkh')], ['id', 'name'])
df.show()
```

- Fetch intermediate data in DB and also error detected until now. This we will filter out so that it doesnot apper in current round of work.

```python
print("Fetchng already saved data from DB.")
ddf = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost/spark").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "my_data").option("user", "root").option("password", "Admin@123").load()
ddf.show()
df = df.subtract(ddf)

if os.path.exists("/root/spark/error"):
    print("Already detected as error.")
    edf = spark.read.format("parquet").load("file:///root/spark/error")
    edf.show()
    df = df.subtract(edf)

print("Data to be considered in this execution")
df.cache()
df.show()
```

> Note above we save error record in local file system as we are running Spark in local mode. Update your code accordingly when running in distributed mode.

- Break current dataset in two half and try save Dataframe into DB and identify error recursively.


```python
print("Save to MySQL")
prop = { "user": "root", "password": "Admin@123", "driver": "com.mysql.jdbc.Driver" }
#df.write.jdbc("jdbc:mysql://localhost/spark", "my_data", mode="append", properties=prop)

def check_error_half(fdf):
    dc = fdf.count()
    ss = math.ceil(dc/2) if dc > 2 else 1
    print("Current iteration split size - {}".format(ss))
    fhdf = fdf.limit(ss)
    tdf = fhdf.localCheckpoint()
    try:
        print("Writing first half.")
        tdf.write.jdbc("jdbc:mysql://localhost/spark", "my_data", mode="append", properties=prop)
    except:
        print("Failure!")
        return tdf
    return fdf.subtract(tdf).localCheckpoint()

while df.count() > 1:
    df.show()
    df = check_error_half(df)

print("Handling final row:")
try:
    df.write.jdbc("jdbc:mysql://localhost/spark", "my_data", mode="append", properties=prop)
    print("SUCCESS: No error row detected!")
except:
    df.show()
    df.write.format("parquet").mode("append").save("file:///root/spark/error")
    print("UNSUCCESSFUL: Error row found!")
```

> After execution if all record is good you will finish without error else you will be able to detect next bad record from dataset.

