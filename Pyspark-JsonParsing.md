# Parsing Json

JSON being one the most widely used text based encoding format, thus most of the data transfer today are primarily done using Json. Thus it is very important to understand the process to load and denormalize Json for further analysis.

How ever is the complexity of the Json it will the following import would be required in any case.

```
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, LongType, IntegerType, DoubleType
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



