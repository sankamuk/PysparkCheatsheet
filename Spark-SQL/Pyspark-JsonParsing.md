# Parsing Json

JSON being one the most widely used text based encoding format, thus most of the data transfer today are primarily done using Json. Thus it is very important to understand the process to load and denormalize Json for further analysis.

How ever is the complexity of the Json it will the following import would be required in any case.

```
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, LongType, IntegerType, DoubleType
from pyspark.sql.functions import col, expr, explode
```

## Use case 1

Our first use case is a simple flat Json.

```
(venv) apple@apples-MacBook-Air data % cat data1.json 
{ "id": 1, "name": "Sankar", "city": "kolkata", "country": "india" }
{ "id": 1, "name": "Kun", "city": "tiangen" }
{ "id": 3, "name": "Jade", "city": "berut", "country": "lebanon" }
{ "id": 4, "name": "Xun", "country": "china" } 

```

Let's define the schema and try to load the data.

```
>>> schema1 = StructType([ StructField('id', LongType()), StructField('name', StringType()), StructField('city', StringType()), StructField('country', StringType()) ])
>>> df1 = spark.read.json('data/data1.json', schema1)
>>> df1.show()
+---+------+-------+-------+
| id|  name|   city|country|
+---+------+-------+-------+
|  1|Sankar|kolkata|  india|
|  1|   Kun|tiangen|   null|
|  3|  Jade|  berut|lebanon|
|  4|   Xun|   null|  china|
+---+------+-------+-------+
```

Note in case your schema is not good you get below outcome:

```
>>> schema1 = StructType([ StructField('id', LongType()), StructField('name', LongType()), StructField('city', LongType()), StructField('country', LongType()) ])
>>> df1 = spark.read.json('data/data1.json', schema1)
>>> df1.show()
+---+----+----+-------+                                                         
| id|name|city|country|
+---+----+----+-------+
|  1|null|null|   null|
|  1|null|null|   null|
|  3|null|null|   null|
|  4|null|null|   null|
+---+----+----+-------+
```

## Use case 2

Next is a slightly complex Json with nesting.

```
(venv) apple@apples-MacBook-Air data % cat data2.json 
{ "id": 1, "name": "Sankar", "nameextention": { "middlename": "some", "suname": "mukherjee" }, "city": "kolkata", "country": "india" }
{ "id": 1, "name": "Kun", "nameextention": { "suname": "xui" }, "city": "tiangen" }
{ "id": 3, "name": "Jade", "nameextention": { "middlename": "josh", "suname": "jabber" }, "city": "berut", "country": "lebanon" }
{ "id": 4, "name": "Xun", "country": "china" }
(venv) apple@apples-MacBook-Air data % cat data2.json | head -1 | ~/Softwares/jq .
{
  "id": 1,
  "name": "Sankar",
  "nameextention": {
    "middlename": "some",
    "suname": "mukherjee"
  },
  "city": "kolkata",
  "country": "india"
}
```

Lets define the schema and load it.

```
>>> schema2 = StructType([ StructField('id', LongType()), StructField('name', StringType()),
... StructField('city', StringType()), StructField('country', StringType()), StructField('nameextention', StructType([
... StructField('middlename', StringType()), StructField('suname', StringType()) ]) ) ])
>>> 
>>> df2 = spark.read.json('data/data2.json', schema2)
>>> df2.show()
+---+------+-------+-------+-----------------+
| id|  name|   city|country|    nameextention|
+---+------+-------+-------+-----------------+
|  1|Sankar|kolkata|  india|[some, mukherjee]|
|  1|   Kun|tiangen|   null|          [, xui]|
|  3|  Jade|  berut|lebanon|   [josh, jabber]|
|  4|   Xun|   null|  china|             null|
+---+------+-------+-------+-----------------+
```

Now let denormalize it.

```
>>> df2.select('id', 'name', col( 'nameextention.middlename' ).alias('middlename'), col( 'nameextention.suname' ).alias('surname'), 'city', 'country' ).show()
+---+------+----------+---------+-------+-------+
| id|  name|middlename|  surname|   city|country|
+---+------+----------+---------+-------+-------+
|  1|Sankar|      some|mukherjee|kolkata|  india|
|  1|   Kun|      null|      xui|tiangen|   null|
|  3|  Jade|      josh|   jabber|  berut|lebanon|
|  4|   Xun|      null|     null|   null|  china|
+---+------+----------+---------+-------+-------+
```

## Use case 3

Now lets handle a complex Json.

```
(venv) apple@apples-MacBook-Air data % cat data3.json                             
{ "id": 1, "name": "Sankar", "nameextention": { "middlename": "some", "suname": "mukherjee" }, "city": "kolkata", "country": "india", "cars": [ {"name": "hero", "model": 2022}, { "name": "bmw", "model": 2009 }] }
{ "id": 1, "name": "Kun", "nameextention": { "suname": "xui" }, "city": "tiangen" , "cars": [ {"name": "audi", "model": 2020 }, { "name": "bmw", "model": 2019 }]}
{ "id": 3, "name": "Jade", "nameextention": { "middlename": "josh", "suname": "jabber" }, "city": "berut", "country": "lebanon", "cars": [ {"name": "ford", "model": 2005 }, { "name": "feat", "model": 2015 }] }
{ "id": 4, "name": "Xun", "country": "china", "cars": [ {"name": "ferarri", "model": 2022 }, { "name": "renault", "model": 2000 }] } 
(venv) apple@apples-MacBook-Air data % cat data3.json | head -1 | ~/Softwares/jq .
{
  "id": 1,
  "name": "Sankar",
  "nameextention": {
    "middlename": "some",
    "suname": "mukherjee"
  },
  "city": "kolkata",
  "country": "india",
  "cars": [
    {
      "name": "hero",
      "model": 2022
    },
    {
      "name": "bmw",
      "model": 2009
    }
  ]
}
```

Lets define the schema and load it.

```
>>> schema3 = StructType([ StructField('id', LongType()), StructField('name', StringType()),
... StructField('city', StringType()), StructField('country', StringType()), StructField('nameextention', StructType([
... StructField('middlename', StringType()), StructField('suname', StringType()) ]) ), StructField('cars', ArrayType( StructType([
... StructField('name', StringType()), StructField('model', LongType()) ])) )])
>>> 
>>> df3 = spark.read.json('data/data3.json', schema3)
>>> df3.show()
+---+------+-------+-------+-----------------+--------------------+
| id|  name|   city|country|    nameextention|                cars|
+---+------+-------+-------+-----------------+--------------------+
|  1|Sankar|kolkata|  india|[some, mukherjee]|[[hero, 2022], [b...|
|  1|   Kun|tiangen|   null|          [, xui]|[[audi, 2020], [b...|
|  3|  Jade|  berut|lebanon|   [josh, jabber]|[[ford, 2005], [f...|
|  4|   Xun|   null|  china|             null|[[ferarri, 2022],...|
+---+------+-------+-------+-----------------+--------------------+

```

Now let denormalize it.

```
>>> df31 = df3.select('id', 'name', 'city', 'country', col( 'nameextention.middlename' ).alias('middlename'), 
...             col( 'nameextention.suname' ).alias('surname'), explode('cars').alias('cars'))
>>> df31.show()
+---+------+-------+-------+----------+---------+---------------+
| id|  name|   city|country|middlename|  surname|           cars|
+---+------+-------+-------+----------+---------+---------------+
|  1|Sankar|kolkata|  india|      some|mukherjee|   [hero, 2022]|
|  1|Sankar|kolkata|  india|      some|mukherjee|    [bmw, 2009]|
|  1|   Kun|tiangen|   null|      null|      xui|   [audi, 2020]|
|  1|   Kun|tiangen|   null|      null|      xui|    [bmw, 2019]|
|  3|  Jade|  berut|lebanon|      josh|   jabber|   [ford, 2005]|
|  3|  Jade|  berut|lebanon|      josh|   jabber|   [feat, 2015]|
|  4|   Xun|   null|  china|      null|     null|[ferarri, 2022]|
|  4|   Xun|   null|  china|      null|     null|[renault, 2000]|
+---+------+-------+-------+----------+---------+---------------+

>>> df31.select('id', 'name', 'middlename', 'surname', 'city', 'country', col('cars.name').alias('carname'), col('cars.model').alias('carmodel') ).show()
+---+------+----------+---------+-------+-------+-------+--------+
| id|  name|middlename|  surname|   city|country|carname|carmodel|
+---+------+----------+---------+-------+-------+-------+--------+
|  1|Sankar|      some|mukherjee|kolkata|  india|   hero|    2022|
|  1|Sankar|      some|mukherjee|kolkata|  india|    bmw|    2009|
|  1|   Kun|      null|      xui|tiangen|   null|   audi|    2020|
|  1|   Kun|      null|      xui|tiangen|   null|    bmw|    2019|
|  3|  Jade|      josh|   jabber|  berut|lebanon|   ford|    2005|
|  3|  Jade|      josh|   jabber|  berut|lebanon|   feat|    2015|
|  4|   Xun|      null|     null|   null|  china|ferarri|    2022|
|  4|   Xun|      null|     null|   null|  china|renault|    2000|
+---+------+----------+---------+-------+-------+-------+--------+

```

Now we should be good now in Json parsing and normalization in Spark. Well almost :)



