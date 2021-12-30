# Mapping Dictionary to a Column

There are times when you want to replace column data with a dictionary map. Lets understand the requirement with a example.

Below dataset is a access report of a perticular bank account.

```python
>>> df = spark.createDataFrame([ ('344022', 'debit', 300), ('433456', 'credit', 25), ('355765', 'view', 0) ], ['accno', 'operation', 'amount'])
>>> df.show()
+------+---------+------+
| accno|operation|amount|
+------+---------+------+
|344022|    debit|   300|
|433456|   credit|    25|
|355765|     view|     0|
+------+---------+------+
```

What we want is to catagorize the operations into transactional and non transactional and create a new column.

- Lets create a dictionary for our mapping.

```python
>>> map_data = { 'debit': 'transactional', 'credit': 'transactional', 'view': 'non-transctional' }
>>> map_data
{'debit': 'transactional', 'credit': 'transactional', 'view': 'non-transctional'}

```

- Now lets map this to our dataframe

```python
>>> from itertools import chain
>>> from pyspark.sql.functions import create_map, lit
>>>
>>> mapping = create_map([lit(x) for x in chain(*map_data.items())])
>>> result_df = df.withColumn('type', mapping[df['operation']])
>>> result_df.show()
+------+---------+------+----------------+
| accno|operation|amount|            type|
+------+---------+------+----------------+
|344022|    debit|   300|   transactional|
|433456|   credit|    25|   transactional|
|355765|     view|     0|non-transctional|
+------+---------+------+----------------+
```

Hope that was super easy!
