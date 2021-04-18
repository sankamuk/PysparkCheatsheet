# Pyspark Structured Streaming - Part 3

## State Management

### Stateful and Stateless

- Spark Structured Streaming offers both stateful and stateless transformation.
- Any agreegation are stateful, most narrow transformation are stateless unless we apply watermark.
- Spark executor stores the state in memory and thus we should be careful about state store size.
- Checkpoint directory is used as a failover mechanism for state store.
- Since stateless jobs output depends only on current batch state information thus `Complete` modes invalid for the same.
