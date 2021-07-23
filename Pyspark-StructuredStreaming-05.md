# Pyspark Structured Streaming - Part 5

## Sliding Windows

It is almost similar as per implementation, but logically they are different:

- In sliding window the window boundery overlap, seems like a continuous windows flow over time progressing forward with `slide duration` interval.

- Events in sliding window can be part of aggregation for multiple window operation.

- All property about watermark applies for sliding windows exactly like sliding window.


Pictorial representattion

```
<--- window 1 (10:00 - 10:15) --->.                       (Events at: 10:04, 10:06, 10:11)
<- 5 min -><--- window 2 (10:05 - 10:20) --->             (Events at: 10:06, 10:11)
<-     10 min   -><--- window 3 (10:10 - 10:25) --->      (Events at: 10:11, 10:22, 10:24)
```

## Use Case: Lets try to find max trade within last 15 mins every 5 mins

We start Zookepper and Kafka and create our topic.

```
# Start Zookeeper
(venv) apple@apples-MacBook-Air PysparkStreaming % ../../Softwares/kafka/bin/zookeeper-server-start.sh ../../Softwares/kafka/config/zookeeper.properties
...
[2021-07-23 11:38:51,101] INFO binding to port 0.0.0.0/0.0.0.0:2181 (org.apache.zookeeper.server.NIOServerCnxnFactory)
...

# Start Kafka
(venv) apple@apples-MacBook-Air PysparkStreaming % ../../Softwares/kafka/bin/kafka-server-start.sh ../../Softwares/kafka/config/server.properties 
...
[2021-07-23 11:39:27,528] INFO [KafkaServer id=0] started (kafka.server.KafkaServer)
...

# Create topic
(venv) apple@apples-MacBook-Air PysparkStreaming % ../../Softwares/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic spark-stream-06 --partitions 1 --replication-factor 1 
```

Now lets push messages

```
(venv) apple@apples-MacBook-Air PysparkStreaming % ../../Softwares/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic spark-stream-06
>{ 'id': 'apple', 'amount': 10, 'type': 'buy', 'time': '20-04-2020-10-04' }
>{ 'id': 'apple', 'amount': 5, 'type': 'sell', 'time': '20-04-2020-10-06' }
>{ 'id': 'dell', 'amount': 8, 'type': 'sell', 'time': '20-04-2020-10-11' }
>{ 'id': 'shell', 'amount': 18, 'type': 'buy', 'time': '20-04-2020-10-22' }
>{ 'id': 'wallmart', 'amount': 19, 'type': 'buy', 'time': '20-04-2020-10-24' }
>
```

Now as we already stated the code for sliding window is almost same except below:

```
    agg_df = transformed_df\
    .withWatermark('eventtime', '30 minutes')\
    .groupby(
        window(
            'eventtime', '15 minutes', '5 minutes'
        )
    ).agg( max('amount').alias('maxtrade') )
```

> Note the ***3rd Parameter*** to window function, which is your sliding interval.

 Result is shown below
 
 ```
-------------------------------------------
Batch: 0
-------------------------------------------
+------------------------------------------+--------+
|window                                    |maxtrade|
+------------------------------------------+--------+
|[2020-04-20 09:50:00, 2020-04-20 10:05:00]|10      |
|[2020-04-20 09:55:00, 2020-04-20 10:10:00]|10      |
|[2020-04-20 10:00:00, 2020-04-20 10:15:00]|10      |
+------------------------------------------+--------+

-------------------------------------------
Batch: 2
-------------------------------------------
+------------------------------------------+--------+
|window                                    |maxtrade|
+------------------------------------------+--------+
|[2020-04-20 09:55:00, 2020-04-20 10:10:00]|10      |
|[2020-04-20 10:00:00, 2020-04-20 10:15:00]|10      |
|[2020-04-20 10:05:00, 2020-04-20 10:20:00]|5       |
+------------------------------------------+--------+

-------------------------------------------
Batch: 4
-------------------------------------------
+------------------------------------------+--------+
|window                                    |maxtrade|
+------------------------------------------+--------+
|[2020-04-20 10:00:00, 2020-04-20 10:15:00]|10      |
|[2020-04-20 10:05:00, 2020-04-20 10:20:00]|8       |
|[2020-04-20 10:10:00, 2020-04-20 10:25:00]|8       |
+------------------------------------------+--------+

-------------------------------------------
Batch: 6
-------------------------------------------
+------------------------------------------+--------+
|window                                    |maxtrade|
+------------------------------------------+--------+
|[2020-04-20 10:10:00, 2020-04-20 10:25:00]|18      |
|[2020-04-20 10:15:00, 2020-04-20 10:30:00]|18      |
|[2020-04-20 10:20:00, 2020-04-20 10:35:00]|18      |
+------------------------------------------+--------+

-------------------------------------------
Batch: 8
-------------------------------------------
+------------------------------------------+--------+
|window                                    |maxtrade|
+------------------------------------------+--------+
|[2020-04-20 10:10:00, 2020-04-20 10:25:00]|19      |
|[2020-04-20 10:15:00, 2020-04-20 10:30:00]|19      |
|[2020-04-20 10:20:00, 2020-04-20 10:35:00]|19      |
+------------------------------------------+--------+
 ```
 
 > We only show important batches.
