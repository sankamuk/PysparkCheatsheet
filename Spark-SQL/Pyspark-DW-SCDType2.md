# Slowly Changing Dimension Type 2


In `Dimensional Modelling` for OLAP systems Slowly Changing Dimension (SCD) is a very important concept. In Dimensional Model we have `Fact Table` which contains all the facts or measurable or numerical entities and `Dimension Tables` that contains supporting data providing meaning to numerical data of Fact table. There are diffent implementation type for `Dimension Tables`, one that is `static` (immutable) and the other is `dynamic` (slowly changing).

`Slowly changing dimensions` are the dimensions in which the data changes slowly, rather than changing regularly on a time basis.

`SCD type 2` methodology is implemented where there is need to store historical data in the Dimension table.

`SCD Row Metadata` are additional columns that will be added to original data to manage SCD Type 2 implementation, in our implementation we will add three columns:
- `Start Date` (s_dt): date ***from when*** this row was active
- `End Date` (e_dt): date ***until when*** this row was/is active
- `Is Active` (is_active): boolean whether row is currently active


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
|2|ali|singur|

- The ***result final state*** to be expected is as below:

|id|name|city|
|---|---|---|
|1|dab|kolkata|
|2|ali|singur|
|3|jad|berut|
|3|jad|paris|
|4|fab|paris|


> Note
- Note we preserved both old and new record for changed dimension.
- Its important to understand that for symplicity i have not considered `surrogate key` which uniquely idenify rows(Primary Key) in Dimentinal table and used for referencial integrity with Fact table (Foriegn Key).
- Also note column `id` is the `natual key` and not the primary key.

## Lets Implement

Lets create spark session and get dataframe created for current data and delta to be applied.

```Python
# Create Spark Session
spark = SparkSession.builder.appName('SCD Type 2').enableHiveSupport().getOrCreate()

# Dataframe for current dataset
c_df = spark.createDataFrame([(1, 'dab', 'kolkata'),
                              (2, 'ali', 'singur'),
                              (3, 'jad', 'berut')],
                             ('id', 'name', 'city'))
print("\nCurrent Dataset:")
c_df.show()

# Dataframe for delta
d_df = spark.createDataFrame([(3, 'jad', 'paris'),
                              (4, 'fab', 'paris'),
                              (2, 'ali', 'singur')],
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
+---+----+------+
| id|name|  city|
+---+----+------+
|  3| jad| paris|
|  4| fab| paris|
|  2| ali|singur|
+---+----+------+
```


#### Step 1: Prepare current dataset as a SCD dataset

```Python
c_df_scd = c_df.\
    withColumn("rand", F.round(F.rand()*(10-5)+5,0).cast('int')).\
    withColumn("today", F.current_date()).\
    selectExpr("*", "date_sub(today, rand) as s_dt").\
    drop("today", "rand").\
    withColumnRenamed("id", "s_id").\
    withColumnRenamed("name", "s_name").\
    withColumnRenamed("city", "s_city").\
    withColumn("e_dt", F.add_months(F.current_date(), 10000)).\
    withColumn("is_active", F.lit("Y"))
print("\nCurrent Dataset in SCD Format")
c_df_scd.show()
```

- Expected output

```
Current Dataset in SCD Format
+----+------+-------+----------+----------+---------+
|s_id|s_name| s_city|      s_dt|      e_dt|is_active|
+----+------+-------+----------+----------+---------+
|   1|   dab|kolkata|2021-08-07|2854-12-15|        Y|
|   2|   ali| singur|2021-08-06|2854-12-15|        Y|
|   3|   jad|  berut|2021-08-08|2854-12-15|        Y|
+----+------+-------+----------+----------+---------+
```

#### Step 2: Prepare the dataset that was updated

```Python
join_df = c_df_scd.join(d_df, c_df_scd.s_id == d_df.id, 'inner')
#join_df.show()

up_new_df = join_df.\
    filter((c_df_scd.is_active == "Y") & ((c_df_scd.s_name != d_df.name) | (c_df_scd.s_city != d_df.city))).\
    select("id", "name", "city", "s_dt", "e_dt", "is_active")
print("\nActive Version of Updated Dataset")
up_new_df.show()

up_old_df = join_df.\
    filter((c_df_scd.is_active == "Y") & ((c_df_scd.s_name != d_df.name) | (c_df_scd.s_city != d_df.city))).\
    selectExpr("s_id as id", "s_name as name", "s_city as city", "s_dt").\
    withColumn("e_dt", F.current_date()).\
    withColumn("is_active", F.lit("N"))
print("\nHistorical Version of Updated Dataset")
up_old_df.show()
```

- Expected output

```
Active Version of Updated Dataset
+---+----+-----+----------+----------+---------+
| id|name| city|      s_dt|      e_dt|is_active|
+---+----+-----+----------+----------+---------+
|  3| jad|paris|2021-08-07|2854-12-15|        Y|
+---+----+-----+----------+----------+---------+


Historical Version of Updated Dataset
+---+----+-----+----------+----------+---------+
| id|name| city|      s_dt|      e_dt|is_active|
+---+----+-----+----------+----------+---------+
|  3| jad|berut|2021-08-07|2021-08-15|        N|
+---+----+-----+----------+----------+---------+
```

# Step 3: Unchanged dataset

```Python
unchg_df = c_df_scd.join(d_df, c_df_scd.s_id == d_df.id, 'left')\
    .filter((d_df.id.isNull()) | (c_df_scd.is_active == "N") | ((c_df_scd.s_name == d_df.name) & (c_df_scd.s_city == d_df.city)))\
    .selectExpr("s_id as id", "s_name as name", "s_city as city", "s_dt", "e_dt", "is_active")
print("\nUnchanged Dataset")
unchg_df.show()
```

- Expected output

```
Unchanged Dataset
+---+----+-------+----------+----------+---------+
| id|name|   city|      s_dt|      e_dt|is_active|
+---+----+-------+----------+----------+---------+
|  1| dab|kolkata|2021-08-10|2854-12-15|        Y|
|  2| ali| singur|2021-08-08|2854-12-15|        Y|
+---+----+-------+----------+----------+---------+
```

# Step 4: New dataset

```Python
new_df = c_df_scd.join(d_df, c_df_scd.s_id == d_df.id, 'right')\
    .filter(c_df_scd.s_id.isNull())\
    .selectExpr("id", "name", "city", "current_date() as s_dt", "'Y' as is_active")\
    .withColumn("e_dt", F.add_months(F.current_date(), 10000))\
    .select("id", "name", "city", "s_dt", "e_dt", "is_active")
print("\nNew Dataset")
new_df.show()
```

- Expected output

```
New Dataset
+---+----+-----+----------+----------+---------+
| id|name| city|      s_dt|      e_dt|is_active|
+---+----+-----+----------+----------+---------+
|  4| fab|paris|2021-08-15|2854-12-15|        Y|
+---+----+-----+----------+----------+---------+
```

# Step 5: Final Dataset

```Python
f_dt = up_old_df.union(up_new_df).union(unchg_df).union(new_df)
print("\nFinal Dataset")
f_dt.show()
```

- Expected output

```
Final Dataset
+---+----+-------+----------+----------+---------+
| id|name|   city|      s_dt|      e_dt|is_active|
+---+----+-------+----------+----------+---------+
|  3| jad|  berut|2021-08-06|2021-08-15|        N|
|  3| jad|  paris|2021-08-06|2854-12-15|        Y|
|  1| dab|kolkata|2021-08-09|2854-12-15|        Y|
|  2| ali| singur|2021-08-08|2854-12-15|        Y|
|  4| fab|  paris|2021-08-15|2854-12-15|        Y|
+---+----+-------+----------+----------+---------+
```
