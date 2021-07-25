# Pivot in Spark

Pivoting is a simple yet powerful analysis mechanism popularized by MS Excel. Spark provides a simple and intutive API to create pivot table as we will see.

Pivot is basically 3 step process:
  - Grouping on set of column
  - Pivoting on set of column
  - Aggregating over the grouped column per pivoted column value


> NOTE: You can specify multiple column for pivoting so each pivot value will be combination of the value concatinated together in the order specified. 
>       Also you can specify a subset of the column as additional argument for pivoting so that all posibble distinc value form the column is not considered.


## Simple example

Lets create a simple data set.

```
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
```

Lets pivot by names.

```
>>> df1.groupBy('age').pivot('name').avg('pay').show()
+---+------+------+------+------+                                               
|age|  jade|   kun|   san|   xun|
+---+------+------+------+------+
| 34|5500.0|  null|  null|  null|
| 32|  null|  null|5000.0|  null|
| 31|  null|  null|  null|6000.0|
| 33|  null|5500.0|  null|  null|
+---+------+------+------+------+

>>> df1.groupBy('age').pivot('name', ['san', 'jade']).avg('pay').show()
+---+------+------+                                                             
|age|   san|  jade|
+---+------+------+
| 34|  null|5500.0|
| 32|5000.0|  null|
| 31|  null|  null|
| 33|  null|  null|
+---+------+------+
```

## A meaninhgful use case

Lets consider a temparature dataset having temparature recording at fixed time for different devices.

```
>>> df11.printSchema()
root
 |-- item: string (nullable = true)
 |-- temp: integer (nullable = true)
 |-- day: date (nullable = true)


>>> df11.show()
+------+----+----------+
|  item|temp|       day|
+------+----+----------+
|fridge| 105|2021-02-01|
| radio|  67|2021-02-01|
|    tv|  73|2021-02-01|
|  oven| 164|2021-02-01|
|fridge|  92|2021-02-02|
| radio|  94|2021-02-02|
|    tv| 187|2021-02-02|
|  oven| 116|2021-02-02|
|fridge|  53|2021-02-03|
| radio| 113|2021-02-03|
|    tv| 215|2021-02-03|
|  oven|  50|2021-02-03|
|fridge|  90|2021-02-04|
| radio| 191|2021-02-04|
|    tv|  14|2021-02-04|
|  oven| 117|2021-02-04|
|fridge| 178|2021-02-05|
| radio| 137|2021-02-05|
|    tv|  38|2021-02-05|
|  oven|  26|2021-02-05|
+------+----+----------+
only showing top 20 rows
```

`Problem` : Now the analysis we want to do is for **each device** what is the **avarage temparature** for different **day of week**.

Lets add a day of week column for each row:

```
>>> from pyspark.sql.functions import col, window, explode, max, min, rank, lag, lead, count, avg, to_date, row_number, date_format
>>> df111 = df11.withColumn( 'dayofweek', date_format(col('day'), 'E') )
>>> df111.show()
+------+----+----------+---------+
|  item|temp|       day|dayofweek|
+------+----+----------+---------+
|fridge| 105|2021-02-01|      Mon|
| radio|  67|2021-02-01|      Mon|
|    tv|  73|2021-02-01|      Mon|
|  oven| 164|2021-02-01|      Mon|
|fridge|  92|2021-02-02|      Tue|
| radio|  94|2021-02-02|      Tue|
|    tv| 187|2021-02-02|      Tue|
|  oven| 116|2021-02-02|      Tue|
|fridge|  53|2021-02-03|      Wed|
| radio| 113|2021-02-03|      Wed|
|    tv| 215|2021-02-03|      Wed|
|  oven|  50|2021-02-03|      Wed|
|fridge|  90|2021-02-04|      Thu|
| radio| 191|2021-02-04|      Thu|
|    tv|  14|2021-02-04|      Thu|
|  oven| 117|2021-02-04|      Thu|
|fridge| 178|2021-02-05|      Fri|
| radio| 137|2021-02-05|      Fri|
|    tv|  38|2021-02-05|      Fri|
|  oven|  26|2021-02-05|      Fri|
+------+----+----------+---------+
only showing top 20 rows
```

Now lets pivot.

```
>>> df111.groupBy('item').pivot('dayofweek').avg('temp').show()
+------+------------------+------------------+------------------+-----+------------------+------------------+-----+
|  item|               Fri|               Mon|               Sat|  Sun|               Thu|               Tue|  Wed|
+------+------------------+------------------+------------------+-----+------------------+------------------+-----+
|  oven|             139.0|108.33333333333333| 94.66666666666667| 81.0|             127.0|             142.0| 49.0|
|    tv|105.66666666666667|             104.0|155.33333333333334|138.5|136.33333333333334|107.33333333333333|182.0|
| radio|             108.0|             144.0|192.33333333333334|118.0|105.66666666666667|              74.0|119.0|
|fridge|147.33333333333334|114.66666666666667|140.66666666666666| 85.0|             126.0|152.66666666666666|107.0|
+------+------------------+------------------+------------------+-----+------------------+------------------+-----+
```

Now if we only want to view the avarage over weekend we do as below.

```
>>> df111.groupBy('item').pivot('dayofweek', ['Sat', 'Sun']).avg('temp').show()
+------+------------------+-----+                                               
|  item|               Sat|  Sun|
+------+------------------+-----+
|  oven| 94.66666666666667| 81.0|
|    tv|155.33333333333334|138.5|
| radio|192.33333333333334|118.0|
|fridge|140.66666666666666| 85.0|
+------+------------------+-----+
```

> NOTE: Pivoting is no magic, internally engine is doing aggragation over the group for each pivoted value separately, so for large data set pivot over only small set of value.












