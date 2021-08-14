# Unpivot

Spark has nice support for Pivot-ing, i.e. pull rows into column. But it doesnot have support for pulling column into rows.

Lets try to understand the requirement before we look for solution.

***Source Table***

|name|age|salary|
|-----|-----|-----|
|ali|34|4000|
|dab|35|5000|
|jad|37|8000|
|kun|36|7500|

***Target***

|name|col_name|col_val|
|------|------|------|
|ali|age|34|
|ali|salary|4000|
|dab|age|35|
|dab|salary|5000|
|jad|age|37|
|jad|salary|8000|
|kun|age|36|
|kun|salary|7500|

As now we understand the task, lets see the PySpark solution.

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# Create Spark Session
spark = SparkSession.builder.appName('Unpivot Example').enableHiveSupport().getOrCreate()

# Create a Dataframe
s_df = spark.createDataFrame([('dab', 34, 5000),
                              ('ali', 33, 4000),
                              ('jad', 36, 8000),
                              ('kun', 36, 8000)],
                             ('name', 'age', 'salary'))

# Display the data
print("Source:")
s_df.show()

# Create rows from column
t_df = s_df.withColumn('unpivot', F.explode(F.array(F.concat(F.lit('age'),
                                                             F.lit('-'),
                                                             F.col("age")),
                                                    F.concat(F.lit('salary'),
                                                             F.lit('-'),
                                                             F.col('salary')))))

print("Transformed:")
t_df.show()

# Prepare final output
o_df = t_df.withColumn('col_name', F.split(F.col('unpivot'), '-')[0])\
    .withColumn('col_value', F.split(F.col('unpivot'), '-')[1])\
    .drop('unpivot')\
    .drop('age')\
    .drop('salary')

# Show output
print("Output:")
o_df.show()
```

We should below result:

```
Source:
+----+---+------+
|name|age|salary|
+----+---+------+
| dab| 34|  5000|
| ali| 33|  4000|
| jad| 36|  8000|
| kun| 36|  8000|
+----+---+------+

Transformed:
+----+---+------+-----------+
|name|age|salary|    unpivot|
+----+---+------+-----------+
| dab| 34|  5000|     age-34|
| dab| 34|  5000|salary-5000|
| ali| 33|  4000|     age-33|
| ali| 33|  4000|salary-4000|
| jad| 36|  8000|     age-36|
| jad| 36|  8000|salary-8000|
| kun| 36|  8000|     age-36|
| kun| 36|  8000|salary-8000|
+----+---+------+-----------+

Output:
+----+--------+---------+
|name|col_name|col_value|
+----+--------+---------+
| dab|     age|       34|
| dab|  salary|     5000|
| ali|     age|       33|
| ali|  salary|     4000|
| jad|     age|       36|
| jad|  salary|     8000|
| kun|     age|       36|
| kun|  salary|     8000|
+----+--------+---------+
```
