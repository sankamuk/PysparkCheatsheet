
# Pyspark Structured Streaming - Part 4

## Watermarking - Cleanup Timebound Aggregation State Store

Until know we seen how Spark manages state store to give us a time bound window aggregation streaming application. But as we pointed out before this is an infinitly growing store and qickly will become un manggable.
This state store is managed in all executor and driver and if not managed will bring down the whole process with OuOfMemory and other issues. 
Thus its important to let Spark know how to clean this state store up with not needed data (note this will depend on busness requirements), and we do this with ***watermarking***.

With watermarking we let Spark know considering the latest windows how much old window data is irrelevent and data for the same can be dropped from state store.

Thus with watermarking enabled the current active windows is calculated as:

Active Window End Time: MAX( Event Time )
Active Window Start Time: MAX( Event Time ) - Watermark Value

Thus if latest event recieved at `2020-04-20 10:25:00` and Watermark is `30 minutes` then any Window whose end time is earlier than  `2020-04-20 9:55:00` will be dropped.
This window `2020-04-20 9:30:00, 2020-04-20 9:45:00` and earlier will be dropped.

