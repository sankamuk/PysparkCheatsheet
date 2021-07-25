
# Handling Skewed Data 

To execute join the all the rows corresponding to a perticular join key value should be present in one excutor. Thus in case where there are uneven distribution of data with respect to the join key, this cause major performance issue. Pulling major share of the data into one executor removed all benifit of parallelism thus this condition need to be handled to improve performance. 

## Salting

Salting is an common technique to handle skewed data in Spark join. Here the i have two data set.

```
>>> df1 = spark.createDataFrame([(1, 'kolkata'), (1, 'delhi'), (1, 'mumbai'), (1, 'chennai'), (2, 'paris'), (3, 'texas')], ['id', 'city'])
>>> df1.show()
+---+-------+                                                                   
| id|   city|
+---+-------+
|  1|kolkata|
|  1|  delhi|
|  1| mumbai|
|  1|chennai|
|  2|  paris|
|  3|  texas|
+---+-------+

>>> df2 = spark.createDataFrame([(1, 'india'), (1, 'france'), (1, 'us')], ['id', 'country'])
>>> df2.show()
+---+-------+
| id|country|
+---+-------+
|  1|  india|
|  1| france|
|  1|     us|
+---+-------+
```

It is clear that the df1 is skewed with respect to column id. Thus if i join the two data set with key id i am going to encounter performance issue. Below is plain old join example.

```
>>> df1.join(df2, df1.id == df2.id, 'inner').show()
+---+-------+---+-------+
| id|   city| id|country|
+---+-------+---+-------+
|  3|  texas|  3|     us|
|  1|kolkata|  1|  india|
|  1|  delhi|  1|  india|
|  1| mumbai|  1|  india|
|  1|chennai|  1|  india|
|  2|  paris|  2| france|
+---+-------+---+-------+
```

Now we apply salting technique.

```
>>> df1f = df1.withColumn('jk', concat(df1['id'], lit('_'), lit((rand(1) * 10000 % 3).cast('int'))))
>>> df1f.show()
+---+-------+---+
| id|   city| jk|
+---+-------+---+
|  1|kolkata|1_0|
|  1|  delhi|1_1|
|  1| mumbai|1_2|
|  1|chennai|1_2|
|  2|  paris|2_0|
|  3|  texas|3_1|
+---+-------+---+

>>> df21 = df2.selectExpr('id','country','0 as df2k')
>>> df22 = df2.selectExpr('id','country','1 as df2k')
>>> df23 = df2.selectExpr('id','country','2 as df2k')
>>> df24 = df21.union(df22).union(df23)
>>> df2f = df24.withColumn('jk', concat( df24['id'], lit('_'), df24['df2k'] )).drop('df2k')
>>> df2f.show()
+---+-------+---+
| id|country| jk|
+---+-------+---+
|  1|  india|1_0|
|  2| france|2_0|
|  3|     us|3_0|
|  1|  india|1_1|
|  2| france|2_1|
|  3|     us|3_1|
|  1|  india|1_2|
|  2| france|2_2|
|  3|     us|3_2|
+---+-------+---+

>>> df1f.join(df2f, df1f.jk == df2f.jk, 'inner').show()
+---+-------+---+---+-------+---+
| id|   city| jk| id|country| jk|
+---+-------+---+---+-------+---+
|  1|  delhi|1_1|  1|  india|1_1|
|  2|  paris|2_0|  2| france|2_0|
|  1|kolkata|1_0|  1|  india|1_0|
|  1| mumbai|1_2|  1|  india|1_2|
|  1|chennai|1_2|  1|  india|1_2|
|  3|  texas|3_1|  3|     us|3_1|
+---+-------+---+---+-------+---+

```

As you can see since the new join key ('jk') is not skewed any more this will bring back the benifits of parallelism of Spark and scale up performance.

