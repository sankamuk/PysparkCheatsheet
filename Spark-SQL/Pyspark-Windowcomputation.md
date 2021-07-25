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

## Ordering windows and get nth row

Its a very common use case to find the nth (largest, smallest) element from an group. Windowing can solve this query requirement very easyly.

First lets create a window

```
>>> window1 = Window.partitionBy('item').orderBy('temp')
```

> NOTE: Ranking will not work if your wondow is not ordered.

Now lets provide rank to the rows and view a perticular group data.

```
>>> df1111 = df11.withColumn('temprank', rank().over(window1))
>>> 
>>> df1111.filter( df111['item'] == 'oven' ).show()
+----+----+----------+--------+                                                 
|item|temp|       day|temprank|
+----+----+----------+--------+
|oven|  12|2021-02-08|       1|
|oven|  14|2021-02-11|       2|
|oven|  26|2021-02-05|       3|
|oven|  26|2021-02-13|       3|
|oven|  46|2021-02-14|       5|
|oven|  46|2021-02-17|       5|
|oven|  50|2021-02-03|       7|
|oven|  51|2021-02-10|       8|
|oven|  70|2021-02-06|       9|
|oven| 116|2021-02-02|      10|
|oven| 116|2021-02-07|      10|
|oven| 117|2021-02-04|      12|
|oven| 129|2021-02-16|      13|
|oven| 140|2021-02-12|      14|
|oven| 149|2021-02-15|      15|
|oven| 164|2021-02-01|      16|
|oven| 181|2021-02-09|      17|
|oven| 188|2021-02-20|      18|
|oven| 250|2021-02-18|      19|
|oven| 251|2021-02-19|      20|
+----+----+----------+--------+
```

> NOTE: There are duplicate rank as there duplicate value for the ordering column.

In case you want to avoid it use row numbering.

```
>>> df1112 = df11.withColumn('temprow', row_number().over(window1))
>>> df1112.filter( df111['item'] == 'oven' ).show()
+----+----+----------+-------+
|item|temp|       day|temprow|
+----+----+----------+-------+
|oven|  12|2021-02-08|      1|
|oven|  14|2021-02-11|      2|
|oven|  26|2021-02-05|      3|
|oven|  26|2021-02-13|      4|
|oven|  46|2021-02-14|      5|
|oven|  46|2021-02-17|      6|
|oven|  50|2021-02-03|      7|
|oven|  51|2021-02-10|      8|
|oven|  70|2021-02-06|      9|
|oven| 116|2021-02-02|     10|
|oven| 116|2021-02-07|     11|
|oven| 117|2021-02-04|     12|
|oven| 129|2021-02-16|     13|
|oven| 140|2021-02-12|     14|
|oven| 149|2021-02-15|     15|
|oven| 164|2021-02-01|     16|
|oven| 181|2021-02-09|     17|
|oven| 188|2021-02-20|     18|
|oven| 250|2021-02-18|     19|
|oven| 251|2021-02-19|     20|
+----+----+----------+-------+
```

## Geting nth previous or following element in an ordered windows

Lets try to create a column with **2nd** previous to current row value and in case there is none use the value **-1** signifing abcence of such value.

```
>>> df112 = df11.withColumn('templag', lag('temp', 2, -1).over(window1))
>>> df112.filter( df111['item'] == 'oven' ).show()
+----+----+----------+-------+
|item|temp|       day|templag|
+----+----+----------+-------+
|oven|  12|2021-02-08|     -1|
|oven|  14|2021-02-11|     -1|
|oven|  26|2021-02-05|     12|
|oven|  26|2021-02-13|     14|
|oven|  46|2021-02-14|     26|
|oven|  46|2021-02-17|     26|
|oven|  50|2021-02-03|     46|
|oven|  51|2021-02-10|     46|
|oven|  70|2021-02-06|     50|
|oven| 116|2021-02-02|     51|
|oven| 116|2021-02-07|     70|
|oven| 117|2021-02-04|    116|
|oven| 129|2021-02-16|    116|
|oven| 140|2021-02-12|    117|
|oven| 149|2021-02-15|    129|
|oven| 164|2021-02-01|    140|
|oven| 181|2021-02-09|    149|
|oven| 188|2021-02-20|    164|
|oven| 250|2021-02-18|    181|
|oven| 251|2021-02-19|    188|
+----+----+----------+-------+
```

Lets try to create a column with **2nd** following current row value and in case there is none use the value **-1** signifing abcence of such value.

```
>>> df113 = df11.withColumn('templead', lead('temp', 2, -1).over(window1))
>>> df113.filter( df111['item'] == 'oven' ).show()
+----+----+----------+--------+
|item|temp|       day|templead|
+----+----+----------+--------+
|oven|  12|2021-02-08|      26|
|oven|  14|2021-02-11|      26|
|oven|  26|2021-02-05|      46|
|oven|  26|2021-02-13|      46|
|oven|  46|2021-02-14|      50|
|oven|  46|2021-02-17|      51|
|oven|  50|2021-02-03|      70|
|oven|  51|2021-02-10|     116|
|oven|  70|2021-02-06|     116|
|oven| 116|2021-02-02|     117|
|oven| 116|2021-02-07|     129|
|oven| 117|2021-02-04|     140|
|oven| 129|2021-02-16|     149|
|oven| 140|2021-02-12|     164|
|oven| 149|2021-02-15|     181|
|oven| 164|2021-02-01|     188|
|oven| 181|2021-02-09|     250|
|oven| 188|2021-02-20|     251|
|oven| 250|2021-02-18|      -1|
|oven| 251|2021-02-19|      -1|
+----+----+----------+--------+
```

## Computing aggregation between a range inside a window

Lets try to find how many temperate are there between -10 to +20 from current row temparature.

```
>>> window2 = Window.partitionBy('item').orderBy('temp').rangeBetween(-10, 20)
>>> df113 = df11.withColumn('temrange', count('temp').over(window2))
>>> df113.filter( df111['item'] == 'oven' ).show()
>>> 
+----+----+----------+--------+                                                 
|item|temp|       day|templead|
+----+----+----------+--------+
|oven|  12|2021-02-08|       4|
|oven|  14|2021-02-11|       4|
|oven|  26|2021-02-05|       4|
|oven|  26|2021-02-13|       4|
|oven|  46|2021-02-14|       4|
|oven|  46|2021-02-17|       4|
|oven|  50|2021-02-03|       5|
|oven|  51|2021-02-10|       5|
|oven|  70|2021-02-06|       1|
|oven| 116|2021-02-02|       4|
|oven| 116|2021-02-07|       4|
|oven| 117|2021-02-04|       4|
|oven| 129|2021-02-16|       3|
|oven| 140|2021-02-12|       2|
|oven| 149|2021-02-15|       3|
|oven| 164|2021-02-01|       2|
|oven| 181|2021-02-09|       2|
|oven| 188|2021-02-20|       2|
|oven| 250|2021-02-18|       2|
|oven| 251|2021-02-19|       2|
+----+----+----------+--------+
```

> Thus for row `|oven|  14|2021-02-11|       4|` there are 4 temp value rows falling between 14 - 10 = 4 and 14 + 20 = 34 as shown below
> |oven|  12|2021-02-08|       4|
> |oven|  14|2021-02-11|       4|
> |oven|  26|2021-02-05|       4|
> |oven|  26|2021-02-13|       4|

Now there are some special range boundary which can be placed in the rangeBetween function.

`Window.currentRow` Current row
`Window.unboundedPreceding` Starting row in window
`Window.unboundedFollowing` Last row in window

Usage like, how many temparature reading has been recorded falling in range of the **lowest temparature** and **+20** of current value.

```
>>> window2 = Window.partitionBy('item').orderBy('temp').rangeBetween(Window.unboundedPreceding, 20)
>>> df113 = df11.withColumn('tempuntl', count('temp').over(window2))
>>> df113.filter( df111['item'] == 'oven' ).show()
+----+----+----------+--------+
|item|temp|       day|tempuntl|
+----+----+----------+--------+
|oven|  12|2021-02-08|       4|
|oven|  14|2021-02-11|       4|
|oven|  26|2021-02-05|       6|
|oven|  26|2021-02-13|       6|
|oven|  46|2021-02-14|       8|
|oven|  46|2021-02-17|       8|
|oven|  50|2021-02-03|       9|
|oven|  51|2021-02-10|       9|
|oven|  70|2021-02-06|       9|
|oven| 116|2021-02-02|      13|
|oven| 116|2021-02-07|      13|
|oven| 117|2021-02-04|      13|
|oven| 129|2021-02-16|      15|
|oven| 140|2021-02-12|      15|
|oven| 149|2021-02-15|      16|
|oven| 164|2021-02-01|      17|
|oven| 181|2021-02-09|      18|
|oven| 188|2021-02-20|      18|
|oven| 250|2021-02-18|      20|
|oven| 251|2021-02-19|      20|
+----+----+----------+--------+
```

