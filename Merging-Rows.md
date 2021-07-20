# Merging Rows

There are many a times we want to merge multiple rows into one. The trick to do this in Spark is with `first` and `last` function.

Lets create a dataframe:

```
>>> from pyspark.sql import functions as F
>>> df = spark.createDataFrame([ ('san', 32, 'kolkata', None, None), 
...                               ('san', 32, None, 'wb', None), 
...                               ('san', 32, None, None, 'india'), 
...                               ('jad', 33, 'paris', None, None), 
...                               ('jad', 33, None, 'ille de france', None), 
...                               ('jad', 33, None, None, 'france')
...                             ], ['name', 'age', 'city', 'state', 'country'])
```

Now lets view what we created

```
>>> df.show()
+----+---+-------+--------------+-------+                                       
|name|age|   city|         state|country|
+----+---+-------+--------------+-------+
| san| 32|kolkata|          null|   null|
| san| 32|   null|            wb|   null|
| san| 32|   null|          null|  india|
| jad| 33|  paris|          null|   null|
| jad| 33|   null|ille de france|   null|
| jad| 33|   null|          null| france|
+----+---+-------+--------------+-------+
```

If we want to merge rows for each person, we can do something like below:

```
>>> df.groupBy('name', 'age').agg(
...   F.first('city', ignorenulls=True).alias('city'), 
...   F.first('state', ignorenulls=True).alias('city'),
...   F.first('country', ignorenulls=True).alias('city')
... ).show()
+----+---+-------+--------------+------+                                        
|name|age|   city|          city|  city|
+----+---+-------+--------------+------+
| jad| 33|  paris|ille de france|france|
| san| 32|kolkata|            wb| india|
+----+---+-------+--------------+------+
```

