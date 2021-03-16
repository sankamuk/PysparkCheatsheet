# Window Based Computations

## Creating a dummy dataset

Lets create a dummy dataset for our analysis. We create fake temparature reading for home appliences over increamental time window,

```
(venv) apple@apples-MacBook-Air data % echo "date,item,temp" > temp.csv
(venv) apple@apples-MacBook-Air data % for i in {1..20}
for> do
for> for j in fridge radio tv oven
for for> do
for for> if [ $i -le 9 ] ; then
for for then> echo "2021-02-0$i,$j,$(od -An -N1 -i /dev/random | awk '{ print $1 }' | head -1)"
for for then> else
for for else> echo "2021-02-$i,$j,$(od -An -N1 -i /dev/random | awk '{ print $1 }' | head -1)"
for for else> fi
for for> done
for> done >> temp.csv
```

Now view a sample data.

```
(venv) apple@apples-MacBook-Air data % head temp.csv 
date,item,temp
2021-02-01,fridge,105
2021-02-01,radio,67
2021-02-01,tv,73
2021-02-01,oven,164
2021-02-02,fridge,92
2021-02-02,radio,94
2021-02-02,tv,187
2021-02-02,oven,116
2021-02-03,fridge,53
```

## Load data

We load data and convert the date column infered as string to a date which is its actual data type.

```
>>> df1 = spark.read.csv('data/temp.csv', header=True, inferSchema=True)
>>> df1.printSchema()
root
 |-- date: string (nullable = true)
 |-- item: string (nullable = true)
 |-- temp: integer (nullable = true)


>>> df11 = df1.withColumn('day', F.to_date(col('date'), 'yyyy-MM-dd')).drop('date')
>>> df11.printSchema()
root
 |-- item: string (nullable = true)
 |-- temp: integer (nullable = true)
 |-- day: date (nullable = true)
```

## Time windows

Lets get weekly average temparature.

```
>>> df11.groupBy(F.window('day', '1 week')).agg(F.avg('temp').cast('int').alias('weekly_average')).show(truncate=False)
+------------------------------------------+--------------+
|window                                    |weekly_average|
+------------------------------------------+--------------+
|[2021-01-28 05:30:00, 2021-02-04 05:30:00]|108           |
|[2021-02-11 05:30:00, 2021-02-18 05:30:00]|129           |
|[2021-02-18 05:30:00, 2021-02-25 05:30:00]|172           |
|[2021-02-04 05:30:00, 2021-02-11 05:30:00]|108           |
+------------------------------------------+--------------+
```

Now lets make a small modification in query to fetch weekly average temparature per appliance.

```
>>> df11.groupBy(F.window('day', '1 week'), 'item').agg(F.avg('temp').cast('int').alias('weekly_average')).show(truncate=False)
+------------------------------------------+------+--------------+
|window                                    |item  |weekly_average|
+------------------------------------------+------+--------------+
|[2021-01-28 05:30:00, 2021-02-04 05:30:00]|radio |116           |
|[2021-02-11 05:30:00, 2021-02-18 05:30:00]|fridge|138           |
|[2021-02-11 05:30:00, 2021-02-18 05:30:00]|oven  |112           |
|[2021-02-04 05:30:00, 2021-02-11 05:30:00]|fridge|126           |
|[2021-01-28 05:30:00, 2021-02-04 05:30:00]|oven  |111           |
|[2021-01-28 05:30:00, 2021-02-04 05:30:00]|fridge|85            |
|[2021-02-04 05:30:00, 2021-02-11 05:30:00]|oven  |67            |
|[2021-01-28 05:30:00, 2021-02-04 05:30:00]|tv    |122           |
|[2021-02-18 05:30:00, 2021-02-25 05:30:00]|tv    |179           |
|[2021-02-11 05:30:00, 2021-02-18 05:30:00]|radio |123           |
|[2021-02-18 05:30:00, 2021-02-25 05:30:00]|oven  |219           |
|[2021-02-11 05:30:00, 2021-02-18 05:30:00]|tv    |144           |
|[2021-02-04 05:30:00, 2021-02-11 05:30:00]|radio |128           |
|[2021-02-04 05:30:00, 2021-02-11 05:30:00]|tv    |112           |
|[2021-02-18 05:30:00, 2021-02-25 05:30:00]|radio |117           |
|[2021-02-18 05:30:00, 2021-02-25 05:30:00]|fridge|173           |
+------------------------------------------+------+--------------+
```

## Custom window column

Lets try to find out maximum and minimum temparatue per appliance.

```
>>> window1 = Window.partitionBy('item')
>>> df11.withColumn('maxtemp', max('temp').over(window1)).withColumn('mintemp', min('temp').over(window1)).select('item','maxtemp','mintemp').distinct().show()
+------+-------+-------+                                                        
|  item|maxtemp|mintemp|
+------+-------+-------+
|  oven|    251|     12|
|    tv|    246|     14|
| radio|    230|     14|
|fridge|    214|     46|
+------+-------+-------+
only showing top 10 rows
```

