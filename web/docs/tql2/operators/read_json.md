# read_json

Parses an incoming JSON stream into events.

<pre>
<span style={{color: "white"}}>
<span style={{color: "#d2a8ff"}}>read_json </span>
[schema<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>str</span>,
selector<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>str</span>,
schema_only<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>bool</span>,
merge<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>bool</span>,
raw<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>bool</span>,
unflatten<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>str</span>,
sep<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>str</span>,
<br/>  ndjson<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>bool</span>,
gelf<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>bool</span>,
arrays_of_objects<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>str</span>]
</span>
</pre>

## Description

- `schema`: ...
- `selector`: ...
- `schema_only`: ...
- ...

TODO: Remove sep?
TODO: Some general sentences?

Options coming from precise parsers:
- schema
- selector
- schema_only
- merge
- raw (this has JSON specific functionality!)
- unflatten

Other options:
- sep
- ndjson
- gelf
- arrays_of_objects

<!--
<pre>
<span style={{color: "white"}}>
(src_ip<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>ip</span>, src_port<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>int</span>, dst_ip<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>ip</span>, dst_port<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>int</span>, proto<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>str</span>, seed<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>int</span>) <span style={{color: "#ff7b72"}}>-&gt;</span> <span style={{color: "#ffa657"}}>str</span>
</span>
</pre> -->


## TODO
- read_json
- metrics
- every
- quantile
- round
- "{}".format(...)
-



## ...

### `selector`
...


## Examples

```
load_file "events.json"
read_json

load_file "events.json"
read_json schema=...

...
```
