# Pyspark Structured Streaming - Part 7

[Reference Documentation](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#join-operations)

## Join - Outer Joins

### Stream To Static

Spark allow left and right outer joins, just note below:

- ***Left outer join*** allowed when `left side` is a stream.
- ***Right outer join*** allowed when `right side` is a stream.
- No full outer join support.


### Stream To Stream

Spark allow left and right outer joins, just note below:

- ***Left outer join*** allowed when `watermark-ing is done` and `event-time constraints` must be specified.
- ***Right outer join*** allowed when `watermark-ing is done` and `event-time constraints` must be specified.
- No full outer join support.

### Event-Time Constrains

Using this he engine can figure out when old rows of one input is not going to be required (i.e. will not satisfy the time constraint) for matches with the other input.
Implemented as, Time range join conditions (e.g. ...JOIN ON leftTime BETWEEN rightTime AND rightTime + INTERVAL 1 HOUR).

- Example:

Let’s say we want to join a stream of advertisement impressions (when an ad was shown) with another stream of user clicks on advertisements to correlate when impressions led to monetizable clicks. To allow the state cleanup in this stream-stream join, you will have to specify the watermarking delays and the time constraints as follows.

Watermark delays: Say, the impressions and the corresponding clicks can be late/out-of-order in event-time by at most 2 and 3 hours, respectively.

Event-time range condition: Say, a click can occur within a time range of 0 seconds to 1 hour after the corresponding impression.

The code would look like this.

```
# Apply watermarks on event-time columns
impressionsWithWatermark = impressions.withWatermark("impressionTime", "2 hours")
clicksWithWatermark = clicks.withWatermark("clickTime", "3 hours")

# Join with event-time constraints
impressionsWithWatermark.join(
  clicksWithWatermark,
  expr("""
    clickAdId = impressionAdId AND
    clickTime >= impressionTime AND
    clickTime <= impressionTime + interval 1 hour
    """)
)
```

