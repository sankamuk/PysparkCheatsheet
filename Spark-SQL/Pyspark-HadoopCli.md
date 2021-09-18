# Execute Hadoop Cli

There exists many a time when you would like to execute Hadoop [Cli](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/CommandsManual.html) from your Pyspark code.
People generally resort to running bash shell command which meaninglessly launches a new JVM as a new process. In this article we try to demonstrate a more native solution.


## Get the HDFS filesystem object


```python
uri = sc._gateway.jvm.java.net.URI
path = sc._gateway.jvm.org.apache.hadoop.fs.Path
fso = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
fs = fso.get(URI("hdfs://quickstart.cloudera:8020"), sc._jsc.hadoopConfiguration())
```

> Note above `hdfs://quickstart.cloudera:8020` is our HDFS URI


Now use it to do your requested operation. Below i show the most used operation, one can try [others](https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/fs/FileSystem.html).


### Create a HDFS directory


```python
fs.isFile(Path('/tmp/logss'))
False
fs.mkdirs(Path('/tmp/logss'))
True
fs.isDirectory(Path('/tmp/logss'))
True
```

### Validate HDFS directory


```python
>>> fs.exists(Path('/tmp/logss'))
True
```

### Delete HDFS directory


```python
fs.isFile(Path('/tmp/logss'))
True
fs.delete(Path('/tmp/logss'), True)
True
fs.isDirectory(Path('/tmp/logss'))
False
```

