
## RDD

### General Operations

- Set loglevel

>>> spark.sparkContext.setLogLevel('ERROR')
>>> rdd.subtract(rdd1).collect()
[54, 7, 56, 28]

>>> log4j = sc._jvm.org.apache.log4j
>>> log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

- Psutil

/Users/apple/TEST/spark/spark-kubernetes-master/BUILD_BINARY/spark/spark2/python/lib/pyspark.zip/pyspark/shuffle.py:60: UserWarning: Please install psutil to have better support with spilling

(mypython) apples-MacBook-Air:bin apple$ pip install psutil

- Help

>>> help(rdd1.takeOrdered)

Help on method takeOrdered in module pyspark.rdd:

takeOrdered(num, key=None) method of pyspark.rdd.RDD instance
    Get the N elements from an RDD ordered in ascending order or as
    specified by the optional key function.
...


### 1. Create RDD

- From list

>>> sc = spark.sparkContext
>>> sc.parallelize([1, 2, 3])
ParallelCollectionRDD[0] at parallelize at PythonRDD.scala:195
>>> rdd1 = sc.parallelize([1, 2, 3])
>>>

- From list with explicit Partition number

>>> rdd = sc.parallelize([1, 2, 3, 7, 56, 54, 28], 2)
>>> rdd.getNumPartitions()
2

- From File/Folder

>>> rdd2 = sc.textFile('/Users/apple/derby.log')
>>>

- sc.textFile can be used to create RDD from local file(local mode) or from shared filesystem(in all node in cluster).
- sc.textFile can load from hdfs://, s3n://
- sc.textFile can load all files from directory if argument is directory
- sc.textFile is an lazy operation thus it will only validate file on Action triggered

### 1. RDD Transformation

- Map

>>> rdd1.map(lambda x: x * 2).collect()
[2, 4, 6]                                                                       
>>>

>>> def mapfunc(x) :
...   return x * 2
... 
>>> rdd1.map(mapfunc).collect()
[2, 4, 6]
>>>

- Flatmap

>>> rdd3 = sc.parallelize(['San Jad Kun', 'IT Accounts Law', '2000 5000 3000'])
>>> rdd3.collect()
['San Jad Kun', 'IT Accounts Law', '2000 5000 3000']
>>> rdd3.map(lambda x: x.split(' ')).collect()
[['San', 'Jad', 'Kun'], ['IT', 'Accounts', 'Law'], ['2000', '5000', '3000']]

>>> rdd3.flatMap(lambda x: x.split(' ')).collect()
['San', 'Jad', 'Kun', 'IT', 'Accounts', 'Law', '2000', '5000', '3000']
>>>

- Filter

>>> rdd1.filter(lambda x: x % 2).collect()
[1, 3]
>>> rdd1.filter(lambda x: x % 2 == 0 ).collect()
[2]

- MapPartition

>>> rdd2.getNumPartitions()
2

>>> def partition_fuc(p) :
...   print(datetime.datetime.now())
...   result = []
...   for i in p:
...     result.append(i)
...   return iter(result)
... 
>>> rdd1.mapPartitions(partition_fuc).collect()
2020-02-15 12:25:26.057355
2020-02-15 12:25:26.064719
2020-02-15 12:25:26.084330
2020-02-15 12:25:26.095890
[1, 2, 3]


- MapPartitionWithIndex

>>> def partition_fuc(i, p) :
...   print('Partition:' + str(i))
...   result = []
...   for i in p:
...     result.append(i)
...   return iter(result)
... 
>>> rdd1.mapPartitionsWithIndex(partition_fuc).collect()
Partition:0
Partition:2
Partition:3
Partition:1
[1, 2, 3]

>>> def partition_fuc(i, p) :
...   result = []
...   for e in p:
...     result.append((i, e))
...   return iter(result)
... 
>>> rdd1.mapPartitionsWithIndex(partition_fuc).collect()
[(1, 1), (2, 2), (3, 3)]


- Union

>>> rdd.collect()
[1, 2, 3, 7, 56, 54, 28]
>>> rdd1.collect()
[1, 2, 3]
>>> 
>>> rdd.union(rdd1).collect()
[1, 2, 3, 7, 56, 54, 28, 1, 2, 3]

> Note duplicate remains

- Intersection

>>> rdd.union(rdd1).intersection(rdd1).collect()
[1, 2, 3]

> No duplicate

- Substract

>>> rdd.subtract(rdd1).collect()
[54, 7, 56, 28]


- Distinct

>>> rdd1 = sc.parallelize([2, 6, 9, 5], 2)
>>> rdd2 = sc.parallelize([2, 15, 94, 62], 2)
>>> rdd2.union(rdd1).collect()
[2, 15, 94, 62, 2, 6, 9, 5]

>>> rdd2.union(rdd1).distinct().collect()
[9, 5, 2, 94, 62, 6, 15]

- Sample

>>> rdd2.union(rdd1).distinct().sample(True, 0.6)
PythonRDD[40] at RDD at PythonRDD.scala:53
>>> rdd2.union(rdd1).distinct().sample(True, 0.6).collect()
[5, 2, 62, 15, 15]
>>> rdd2.union(rdd1).distinct().sample(False, 0.6).collect()
[9, 5, 2, 94, 62, 6]

> First parameter withReplacement allow element to reappear.


>>> rdd2.union(rdd1).distinct().sample(False, 0.5, 3).collect()
[2, 62]
>>> rdd2.union(rdd1).distinct().sample(False, 0.5).collect()
[9, 15]
>>> rdd2.union(rdd1).distinct().sample(False, 0.5, 3).collect()
[2, 62]

> Seed allow you to repeat random selection


### 2. RDD Action

- Count

>>> rdd3 = sc.textFile('/Users/apple/derby.log')
>>> rdd3.count()
13

- First

>>> rdd3.first()
'----------------------------------------------------------------'

> First row of first partition
> If empty throw exception

- Take

>>> rdd3.take(3)
['----------------------------------------------------------------', 'Sun Dec 29 17:05:06 CET 2019:', 'Booting Derby version The Apache Software Foundation - Apache Derby - 10.12.1.1 - (1704137): instance a816c00e-016f-5267-67a7-00000aedfc88 ']

> Collect partition by partition

- Reduce

>>> rdd1.reduce(lambda x, y: x+y)
22

> Aggregate all elements
> Function should be commutative and associative, eg sum, multiply but not substract.

- Sample Action

>>> rdd1.takeSample(True, 5)
[6, 2, 2, 2, 2]

- Ordered Number

>>> rdd1.takeOrdered(3)
[2, 5, 6]

- Top

>>> rdd1.top(3)
[9, 6, 5]


- Save As Text File

>>> rdd3.saveAsTextFile('/Users/apple/TEST/spark/output')
>>>

(mypython) apples-MacBook-Air:~ apple$ cat /Users/apple/TEST/spark/output/
._SUCCESS.crc    .part-00000.crc  .part-00001.crc  _SUCCESS         part-00000       part-00001       
(mypython) apples-MacBook-Air:~ apple$ cat /Users/apple/TEST/spark/output/part-0000*
----------------------------------------------------------------
Sun Dec 29 17:05:06 CET 2019:
Booting Derby version The Apache Software Foundation - Apache Derby - 10.12.1.1 - (1704137): instance a816c00e-016f-5267-67a7-00000aedfc88 


### 3. Pair RDD Transformation

- Creation

>>> sc = spark.sparkContext
>>> rdd = sc.parallelize(['san', 'jade', 'xun', 'ku', 'guam', 'slima'], 2)
>>> prdd = rdd.map(lambda x: (len(x), x))
>>> prdd.collect()
[(3, 'san'), (4, 'jade'), (3, 'xun'), (2, 'ku'), (4, 'guam'), (5, 'slima')] 

- Group by Key

>>> sc.setLogLevel('ERROR')
>>> prdd.groupByKey().collect()
[(4, <pyspark.resultiterable.ResultIterable object at 0x10f344c50>), (2, <pyspark.resultiterable.ResultIterable object at 0x10f344a20>), (3, <pyspark.resultiterable.ResultIterable object at 0x10f344978>), (5, <pyspark.resultiterable.ResultIterable object at 0x10f344748>)]

- Reduce by Key

>>> rdd.map(lambda x: (len(x), 1)).reduceByKey(lambda x, y: x+1).collect()
[(4, 2), (2, 1), (3, 2), (5, 1)]

- Sort by Key

>>> rdd.map(lambda x: (len(x), 1)).sortByKey().collect()
[(2, 1), (3, 1), (3, 1), (4, 1), (4, 1), (5, 1)]


- Join

>>> r1 = rdd.map(lambda x: (x, len(x) * 10))
>>> r2 = rdd.map(lambda x: (x, len(x)))

>>> r1.join(r2).collect()
[('jade', (40, 4)), ('xun', (30, 3)), ('guam', (40, 4)), ('san', (30, 3)), ('ku', (20, 2)), ('slima', (50, 5))]
>>>

### 4. Pair RDD Action

- Collect as Map

>>> rdd.map(lambda x: (len(x), x)).collectAsMap()
{3: 'xun', 4: 'guam', 2: 'ku', 5: 'slima'}

- Count by Key

>>> rdd.map(lambda x: (len(x), x)).countByKey()
defaultdict(<class 'int'>, {3: 2, 4: 2, 2: 1, 5: 1})


- Lookup by Key

>>> rdd.map(lambda x: (x, len(x))).collect()
[('san', 3), ('jade', 4), ('xun', 3), ('ku', 2), ('guam', 4), ('slima', 5)]
>>> rdd.map(lambda x: (x, len(x))).lookup('ku')
[2]


> Certain key/value transformations and actions require moving data from multiple partitions to other partitions, meaning across executors and machines. This process is known as the shuffle, which is quite important to be familiar with because it is an expensive operation. During the shuffling process, the data needs to be serialized, written to disk, transferred over the network, and finally deserialized. It is not possible to completely avoid the shuffling.



