# Pyspark Structured Streaming - Part 01

Below is the issues with classic Spark batch jobs to handle streaming by running frequent batches over the data.

- Should we run in incremental fashion and save and manage the output of all previous batches. Or we treat every batch idempotent and process all data from the very begining until the current batch.
- Should we validate whether previous batch state (has it finished) or we do not care.
- Should we validate the data is complete for the current batch or we do not care.
- How do we track the data already processed and need to be processed in current batch.
- Should we bother about the failure that happened in previous batch or not.
- How do we handle late arriving data, data whose actual event time is earlier but reported to system in later time because of various environment or system issue.

Handling all these is not new issue but it just gets complecated when the batch becomes smaller and frequent. 

The elegant solution to all such problems in Spark is Streaming. The idea that Spark creator advocated is Sream processing is just about handing some additional problems cases over the already solved batch processing problems. Thus Spark Streaming API has been build upon the batch processing API with additional capabilities.

Thus Spark Streaming offer the below out of the box.

- Scheduling of batches.
- Data management for batches.
- Intermediate state management across batches.
- Combine states across batches.
- Batch failure and job failure management.

Spark Structured Streaming API add the below features over the old DStream API.

- DStream API is deprecated and no new development to be done with it.
- Unified Dataframe based data processing framework for both batch and streaming data.
- Avalability of Catalyst optimiser which works with SQL engine processing Dataframe.
- Support for EVENT time based processing rather than incorrect process time based processing.



