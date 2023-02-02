---
sidebar_position: 1
---

# Pipelines

A pipeline is chain of [operators](operators) that represents a dataflow. An
operator consumes data, performs a transformation, and produces new data,
possibly with a different schema. Think of it as UNIX pipes where output from
one command is input to the next.

![Query Language](/img/query-language.light.png#gh-light-mode-only)
![Query Language](/img/query-language.dark.png#gh-dark-mode-only)

The basic idea is that a query consists of two connected pieces: a *dataset* to
represent a data source and a *pipeline* as a squence of operators to process
the data.

To date, a VAST [expression](expressions) takes the role of a dataset and you
can only [define a pipeline](/docs/use/transform) statically in the YAML
configuration. Being able to execute pipeline as part of the query is our most
requested feature, and we are actively working on bringing this ability into the
query language.

## Define a pipeline

Add a uniquely named pipeline under the key `vast.pipelines` in the
configuration file:

```yaml
vast:
  pipelines:
     example: |
       hash --salt="B3IwnumKPEJDAA4u" src_ip
       | summarize 
           pkts_toserver=sum(flow.pkts_toserver),
           pkts_toclient=sum(flow.pkts_toclient),
           bytes_toserver=sum(flow.bytes_toserver),
           bytes_toclient=sum(flow.bytes_toclient),
           start=min(flow.start),
           end=max(flow.end)
         by
           src_ip,
           dest_ip
```

The above `example` pipeline consists of two operators, `hash` and `summarize`
that execute in sequential order.

Please consult the [section on data transformation](/docs/use/transform) on
where you can deploy pipelines today. Have a look at [all available
operators](operators) to better understand what you can do with the data.

## Launch pipelines dynamically (experimental)

As an alternative to configuration file-based pipelines, the `export` and
`import` commands support launching a dynamically defined pipeline. The
`export` command and the data loaded by the `import` command will provide the
respective beginning datasets.

This dynamic pipeline is an optional string parameter, with operators chained
by the `|` delimiter. This pipeline can be put after an expression so it will
only be applied to the resulting dataset of that expression. For example:

`export json 'src_ip == 192.168.1.104 | select timestamp, flow_id, src_ip,
dest_ip, src_port | drop timestamp'`

`import -b suricata < data/suricata/eve.json 'src_ip==147.32.84.165 &&
(src_port==1181 || src_prt == 138) | select timestamp, flow_id, src_ip,
dest_ip, src_port'`

Have a look at [all available operators](operators) for more details about the
respective pipeline operator string syntax. Please note that this feature is
experimental and the syntax may be subject to change.
