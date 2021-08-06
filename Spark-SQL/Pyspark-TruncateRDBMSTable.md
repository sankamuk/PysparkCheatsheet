# Truncate RDBMS Table

Often times we face issue in clearing out RDBMS table with which your Spark Application is working with. Below we show how we can truncate a RDBMS table with without any side effect.

## Create and Load Source table

Here we use a MySQL instance and create and load a table.

```
mysql> use userdb ;
Reading table information for completion of table and column names
You can turn off this feature to get a quicker startup with -A

Database changed
mysql> create table emp ( id int, name varchar(50), country varchar(50) );
Query OK, 0 rows affected (0.05 sec)
mysql> insert into emp values (1, 'dabli', 'india');
Query OK, 1 row affected (0.00 sec)

mysql> insert into emp values (1, 'jade', 'france');
Query OK, 1 row affected (0.00 sec)

mysql> insert into emp values (1, 'kun', 'china');
Query OK, 1 row affected (0.01 sec)

mysql> insert into emp values (1, 'xun', 'china');
Query OK, 1 row affected (0.00 sec)

mysql> select * from emp ;
+------+-------+---------+
| id   | name  | country |
+------+-------+---------+
|    1 | dabli | india   |
|    1 | jade  | france  |
|    1 | kun   | china   |
|    1 | xun   | china   |
+------+-------+---------+
5 rows in set (0.00 sec)
```

## Lets create a Spark Session

```
import os                                                                                                                                                                                                                                                                                                     
from pyspark.sql import SparkSession, SQLContext                                                                             
from pyspark.sql.types import StructType, StructField, StringType, IntegerType                                               
                                                                                                                             
# Setting driver path                                                                                                        
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /Users/apple/PycharmProjects/jars/mysql-connector-java-5.1.48.jar pyspark-shell' 
                                                                                                                             
# Create Spark Session                                                                                                       
spark = SparkSession.builder.appName('Test Truncate RDBMS Table').enableHiveSupport().getOrCreate()        

```

## Next we load the data from source and validate

```                                                                                                                             
# Load Data                                                                                                                  
df_mysql_full = spark.read.format("jdbc")\                                                                                   
             .option("url", "jdbc:mysql://192.168.56.107/userdb")\                                                           
             .option("driver", "com.mysql.jdbc.Driver")\                                                                     
             .option("dbtable", "emp")\                                                                                      
             .option("user", "hadoop")\                                                                                      
             .option("password", "Admin@123")\                                                                               
             .load()  

# Show data & dataframe from fully loaded table         
print("Schema (Loaded):")                               
df_mysql_full.printSchema()                             
print("Data (Loaded): ")                                
df_mysql_full.show(truncate=False)                      
```

***Output:***

```
Schema (Loaded):
root
 |-- id: integer (nullable = true)
 |-- name: string (nullable = true)
 |-- country: string (nullable = true)

Data (Loaded): 
+---+-----+-------+
|id |name |country|
+---+-----+-------+
|1  |kun  |china  |
|1  |xun  |china  |
|1  |jade |france |
|1  |dabli|india  |
+---+-----+-------+
```

## Lets try to *truncate* the table

```
# Create blank dataframe                                              
df_mysql_blank = spark.createDataFrame([], df_mysql_full.schema)      
print("Schema (Empty):")                                              
df_mysql_blank.printSchema()                                          
print("Data (Empty): ")                                               
df_mysql_blank.show(truncate=False)                                   
                                                                      
# Clear Loaded table                                                  
df_mysql_blank.write.format("jdbc")\                                  
             .option("url", "jdbc:mysql://192.168.56.107/userdb")\    
             .option("driver", "com.mysql.jdbc.Driver")\              
             .option("dbtable", "emp")\                               
             .option("user", "hadoop")\                               
             .option("password", "Admin@123")\                        
             .option("truncate", "true")\                             
             .mode("overwrite")\                                      
             .save()                                                  
```

***Output:***

```
Schema (Empty):
root
 |-- id: integer (nullable = true)
 |-- name: string (nullable = true)
 |-- country: string (nullable = true)

Data (Empty): 
+---+----+-------+
|id |name|country|
+---+----+-------+
+---+----+-------+
```

## Validate in RDBMS whether table is cleared

```
mysql> select * from userdb.emp ;
Empty set (0.00 sec)

mysql>
```

> Note we created a new ***blank dataframe*** and it have no shared lineage with the dataframe which loaded the data from the source initially.

