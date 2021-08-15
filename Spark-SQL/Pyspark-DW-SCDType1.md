# Slowly Changing Dimension Type 1


In `Dimensional Modelling` for OLAP systems Slowly Changing Dimension (SCD) is a very important concept. In Dimensional Model we have `Fact Table` which contains all the facts or measurable or numerical entities and `Dimension Tables` that contains supporting data providing meaning to numerical data of Fact table.
`Slowly changing dimensions` are the dimensions in which the data changes slowly, rather than changing regularly on a time basis.

`SCD type 1` methodology is implemented where there is no need to store historical data in the Dimension table. This method overwrites the old data in the dimension table with the new data.


In our below use case we follow the pattern of `change data capture` where we will bring in only the new data from our source for each new iteration. 

- Current data


|id|name|city|
|---|---|---|
|1|dab|kolkata|
|2|ali|singur|
|3|jad|berut|


- Delta

|id|name|city|
|---|---|---|
|4|fab|paris|
|3|jad|paris|


- The ***result final state*** to be expected is as below:

|id|name|city|
|---|---|---|
|1|dab|kolkata|
|2|ali|singur|
|3|jad|paris|
|4|fab|paris|

> Note as its type1 scd there is no history for Jad's change of city.



## Lets Implement

Lets create spark session and get dataframe created for current data and delta to be applied.

```Python
# Create Spark Session
spark = SparkSession.builder.appName('SCD Type 1').enableHiveSupport().getOrCreate()

# Dataframe for current dataset
c_df = spark.createDataFrame([(1, 'dab', 'kolkata'),
                              (2, 'ali', 'singur'),
                              (3, 'jad', 'berut')],
                             ('id', 'name', 'city'))
print("\nCurrent Dataset:")
c_df.show()

# Dataframe for delta
d_df = spark.createDataFrame([(3, 'jad', 'paris'),
                              (4, 'fab', 'paris')],
                             ('id', 'name', 'city'))
print("\nDelta Dataset")
d_df.show()
```

- Expected output

```
Current Dataset:
+---+----+-------+
| id|name|   city|
+---+----+-------+
|  1| dab|kolkata|
|  2| ali| singur|
|  3| jad|  berut|
+---+----+-------+

Delta Dataset
+---+----+-----+
| id|name| city|
+---+----+-----+
|  3| jad|paris|
|  4| fab|paris|
+---+----+-----+
```

> Note in delta we have Jad's record updated while Fab's record is a new addition.

### Lets start to implement solution

We perform solution in few steps.

#### Step 1: Create joined dataset

```Python
join_df = c_df.join(d_df, c_df.id == d_df.id, 'fullouter')
join_df.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
print("\nJoined Dataset")
join_df.show()
```

- Result
```
```

#### Step 2: Get record which are unchanged

```Python
unchg_df = join_df.\
    filter(d_df.id.isNull()).\
    select(c_df.id, c_df.name, c_df.city)
print("\nUnchanged Dataset")
unchg_df.show()
```

- Result
```
+---+----+-------+
| id|name|   city|
+---+----+-------+
|  1| dab|kolkata|
|  2| ali| singur|
+---+----+-------+
```

#### Step 3: Get records to be newly inserted

```Python
new_df = join_df.\
    filter(c_df.id.isNull()).\
    select(d_df.id, d_df.name, d_df.city)
print("\nNew Dataset")
new_df.show()
```

- Result
```
+---+----+-----+
| id|name| city|
+---+----+-----+
|  4| fab|paris|
+---+----+-----+
```

#### Step 4: Get records to be updated

```Python
up_df = join_df.\
    filter(d_df.id.isNotNull()).\
    select(c_df.id, d_df.name, d_df.city)
print("\nUpdated Dataset")
up_df.show()
```

- Result
```
+---+----+-----+
| id|name| city|
+---+----+-----+
|  3| jad|paris|
+---+----+-----+
```

#### Step 5: Result dataset

```Python
f_df = unchg_df.union(new_df).union(up_df)
print("\nFinal Dataset")
f_df.show()
```

- ***Final Result***
```
+---+----+-------+
| id|name|   city|
+---+----+-------+
|  1| dab|kolkata|
|  2| ali| singur|
|  4| fab|  paris|
|  3| jad|  paris|
+---+----+-------+
```

