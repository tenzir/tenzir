---
sidebar_position: 1
---

# Pipelines

A pipeline is chain of [operators](operators) that represents a dataflow. An
operator consumes data, performs a transformation, and produces new data,
possibly with a different schema. Think of it as UNIX pipes where output from
one command is input to the next.

## Define a pipeline

Add a uniquely named pipeline under the key `vast.pipelines` in the
configuration file:

```yaml
vast:
  pipelines:
     example:
       - hash:
           field: src_ip
           out: pseudonym
           salt: "B3IwnumKPEJDAA4u"
       - summarize:
           group-by:
             - src_ip
             - dest_ip
           aggregate:
             flow.pkts_toserver: sum
             flow.pkts_toclient: sum
             flow.bytes_toserver: sum
             flow.bytes_toclient: sum
             flow.start: min
             flow.end: max
```

The above `example` pipeline consists of two operators, `hash` and `summarize`
that execute in sequential order.
