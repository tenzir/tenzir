# metrics

Brief description.

<pre>
<span style={{color: "white"}}>
<span style={{color: "#d2a8ff"}}>metrics </span>
name<span style={{color: "#ff7b72"}}>?:</span><span style={{color: "#ffa657"}}>str</span>,
retro<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>bool</span>,
live<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>bool</span>,
parallel<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>int</span>
</span>
</pre>


<pre>
<span style={{color: "white"}}>
<span style={{color: "#d2a8ff"}}>metrics </span>
[name<span style={{color: "#ff7b72"}}>:</span><span style={{color: "#ffa657"}}>str</span>,
retro<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>bool</span>,
live<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>bool</span>,
parallel<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>int</span>]
</span>
</pre>


### Description

TODO

### Examples

```
// Same as `retro=true, live=false`.
metrics "TODO"

// Same as `retro=false, live=true`.
metrics "TODO", live=true

// Both at the same time.
metrics "TODO", retro=true, live=true
```

### TODO: Schemas

Tenzir collects metrics with the following schemas (TODO: name/schema?)

TODO: Consider reducing header level (everywhere).

#### `tenzir.metrics.lookup`

Contains a measurement of the `lookup` operator, emitted once every second.

```
{
  /// The ID of the pipeline where the associated operator is from.
  pipeline_id: string,
  /// The number of the run, starting at 1 for the first run.
  run: uint64
  /// True if the pipeline is running for the explorer.
  hidden: bool
  /// The time at which this metric was recorded.
  timestamp: time
  /// The ID of the `lookup` operator in the pipeline.
  operator_id: uint64
  /// The name of the context the associated operator is using.
  context: string,
  /// Information about the live lookup.
  live: {
    /// The amount of input events used for the live lookup since the last metric.
    events: uint64,
    /// The amount of live lookup matches since the last metric.
    hits: uint64,
  },
  /// Information about the retroactive lookup.
  retro: {
    /// The amount of input events used for the lookup since the last metric.
    events: uint64,
    /// The amount of lookup matches since the last metric.
    hits: uint64,
    /// The total amount of events that were in the queue for the lookup.
    queued_events: uint64,
  },
  /// The amount of times the underlying context has been updated while the associated lookup is active.
  context_updates: uint64,
}
```
