# Data Cleaning

In this note we try to explain the process of data cleaning.

## Cleaning JSON data

Lets look at our input data.

```
(venv) apple@apples-MacBook-Air data % cat json.data 
{ "id": 1, "name": "Sankar", "city": "kolkata", "country": "india" }
{ "id": 2, "name": "Kun", "city": "tiangen" }
{ "id": 3, "name": "Jade", "city": "berut", "country": "lebanon" }
{ "id": 8, "name": "Slim
{ "id": 4, "name": "Xun", "country": "china" } 
{ "id": err, "name": "Dui", "country": "china" }
```

The data has lot of inconsistency. The below list explains the type and how Spark and then we try to clean and handle those.

- Incomplete rows. This type of row will not parse as an valid JSON, thus Spark will detect and try will not be able to create a valid row.
- Datatype mismatch. If few column for specific row does not match our expected schema defination Spark will fail in parsing and also in auto type casting. Thus no rwo could be created.
- Column missing. Row will be created with the missing column as NULL.

> Note absense of data in key columns will make our row invalid logically thus we need to mark them as bad data.

***Goal*** We need to parse the data and create two Dataframe one for *GOOD* data another for *BAD* data.

***Load Input***

```
>>> df = spark.read.text('data/json.data')
>>> df.show(truncate=False)
+--------------------------------------------------------------------+          
|value                                                               |
+--------------------------------------------------------------------+
|{ "id": 1, "name": "Sankar", "city": "kolkata", "country": "india" }|
|{ "id": 2, "name": "Kun", "city": "tiangen" }                       |
|{ "id": 3, "name": "Jade", "city": "berut", "country": "lebanon" }  |
|{ "id": 8, "name": "Slim                                            |
|{ "id": 4, "name": "Xun", "country": "china" }                      |
|{ "id": err, "name": "Dui", "country": "china" }                    |
+--------------------------------------------------------------------+
```

***Parse Data***

```
>>> schema1 = StructType([ StructField('id', LongType()), StructField('name', StringType()), StructField('city', StringType()), StructField('country', StringType()) ])
>>> 
>>> df.select( from_json(df.value, schema1).alias("json") ).printSchema()
root
 |-- json: struct (nullable = true)
 |    |-- id: long (nullable = true)
 |    |-- name: string (nullable = true)
 |    |-- city: string (nullable = true)
 |    |-- country: string (nullable = true)

>>> df1 = df.select( df.value, from_json(df.value, schema1).alias("json") )
>>> df1.select( df1.value, col( 'json.id' ).alias('id'), col( 'json.name' ).alias('name'), col( 'json.country' ).alias('country'), col( 'json.city' ).alias('city') ).show() 
+--------------------+----+------+-------+-------+
|               value|  id|  name|country|   city|
+--------------------+----+------+-------+-------+
|{ "id": 1, "name"...|   1|Sankar|  india|kolkata|
|{ "id": 2, "name"...|   2|   Kun|   null|tiangen|
|{ "id": 3, "name"...|   3|  Jade|lebanon|  berut|
|{ "id": 8, "name"...|null|  null|   null|   null|
|{ "id": 4, "name"...|   4|   Xun|  china|   null|
|{ "id": err, "nam...|null|  null|   null|   null|
+--------------------+----+------+-------+-------+

>>> df2 = df1.select( df1.value, col( 'json.id' ).alias('id'), col( 'json.name' ).alias('name'), col( 'json.country' ).alias('country'), col( 'json.city' ).alias('city') )
```

Now its time to seggregte the bad and good data.

***Good Data***

```
>>> df2.na.drop( 'any', subset=['id', 'name'] ).show()
+--------------------+---+------+-------+-------+
|               value| id|  name|country|   city|
+--------------------+---+------+-------+-------+
|{ "id": 1, "name"...|  1|Sankar|  india|kolkata|
|{ "id": 2, "name"...|  2|   Kun|   null|tiangen|
|{ "id": 3, "name"...|  3|  Jade|lebanon|  berut|
|{ "id": 4, "name"...|  4|   Xun|  china|   null|
+--------------------+---+------+-------+-------+
```

***Bad Data***

```
>>> df2.filter( ( col('id').isNull() ) | ( col('name').isNull() ) ).show()
+--------------------+----+----+-------+----+
|               value|  id|name|country|city|
+--------------------+----+----+-------+----+
|{ "id": 8, "name"...|null|null|   null|null|
|{ "id": err, "nam...|null|null|   null|null|
+--------------------+----+----+-------+----+
```

## Cleaning CSV data


