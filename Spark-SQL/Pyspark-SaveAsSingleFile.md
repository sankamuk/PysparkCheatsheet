# Save Spark DataFrame to a Single file

Spark working in distributed mode always saves Dataset as a directory, with a Status file (i.e. \_SUCCESS for success write) and a set of datafiles of the format configured for the DataFrameWriter.

```
output_directory
  |
  +--- _SUCCESS
  +--- part-*.[Format]
```

Now many times we get requirement to save Spark DataFrame as a file to any target (could be local HDFS or even a remote S3). In this topic we try to perfomr this operation with Pyspark.

## Requirement

We will be writing a Spark Dataframe to a remote S3 (feel this will be triky as its not the local HDFS which would have been simpler) as a CSV file with custom name and directory inside the bucket.

- Process
  - Load the DataFrame
  - Configure Spark context to write to a remote S3 bucket
  - Write dataframe as a single datafile in a temporary directory in S3 bucket
  - Validate whether the write was successful checking for the presence of a \_SUCCESS file in output directory
  - Remane the temporary file created above to the final destination file
  - Delete the temporary directory

## Implementation

Lets handle the problem set by step.

### Load DataFrame

```python
>>> df = spark.createDataFrame([ ('san', 32, 'kolkata', None, None), 
...                               ('san', 32, None, 'wb', None), 
...                               ('san', 32, None, None, 'india'), 
...                               ('jad', 33, 'paris', None, None), 
...                               ('jad', 33, None, 'ille de france', None), 
...                               ('jad', 33, None, None, 'france')
...                             ], ['name', 'age', 'city', 'state', 'country'])
```

### Configure Spark Context to Write to S3

```python
sc = spark.SparkContext
sc.setSystemProperty('com.amazonaws.services.s3.enableV4', 'true')
hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set(“fs.s3a.endpoint”, “s3-ap-south-1.amazonaws.com”)
hadoopConf.set(“com.amazonaws.services.s3a.enableV4”, “true”)
hadoopConf.set(“fs.s3a.impl”, “org.apache.hadoop.fs.s3a.S3AFileSystem”)
hadoopConf.set('fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
hadoopConf.set('fs.s3a.path.style.access', 'true')
hadoopConf.set('fs.s3a.acl.default', 'PublicReadWrite')
hadoopConf.set('fs.s3a.connection.maximum', '30')
hadoopConf.set('fs.s3a.server-side-encryption-algorithm', 'AES256')
hadoopConf.set('log4j.logger.org.apache.hadoop.fs.s3a', 'DEBUG')

hadoopConf.set(“fs.s3a.awsAccessKeyId”, “XXXXX”)             # Replace with appropiate value
hadoopConf.set(“fs.s3a.awsSecretAccessKey”, “XXXXX”)         # Replace with appropiate value
```

Note detail about the configurations could be found [HERE](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)

### Write DataFrame to a Temporary directory in S3

```python
from datetime import datetime
bucket_name = 'your S3 bucket'                              # Replace with appropiate value
s3_bucket_dir = 'directory in S3 bucket to save file to'    # Replace with appropiate value
cur_timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")

tmp_dir = "s3a://{0}/{1}/tmp-{2}".format(bucket_name, s3_bucket_dir, cur_timestamp)
df.coalesce(1).format('csv').save(tmp_dir)
```

### Validate write

```python
import os, sys

URI = sc._gateway.jvm.java.net.URI
PTH = sc._gateway.jvm.org.apache.hadoop.fs.Path
FSH = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem.get(URI("s3a://{}/".format(bucket_name)), sc._jsc.hadoopConfiguration())

if FSH.isDirectory(PTH("{0}/{1}".format(tmp_dir, '_SUCCESS'))):
  print("Successfully saved DatFrame to S3"
else:
  print("Incomplete save. Exiting!!!")
  exit(-1)
```

### Rename temporary data file to target file

```python
target_filename_prefix = 'Finame'                        # Replace with appropiate value

dest_file = "s3a://{0}/{1}/{2}-{3}.csv".format(bucket_name, s3_bucket_dir, target_filename_prefix, cur_timestamp)

src_file_name = FSH.globStatus(PTH("{0}/part*".format(tmp_dir)))[0].getPath().getName()
src_file = "{0}/{1}".format(tmp_dir, src_file_name)

FSH.rename(PTH(src_file), PTH(dest_file))
```

### Delete temporary directory

```python
FSH.delete(PTH(tmp_dir), True)
```

You are done!!!



