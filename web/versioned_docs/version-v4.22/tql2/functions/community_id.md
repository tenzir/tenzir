# community_id

Computes the [Community ID](https://github.com/corelight/community-id-spec) for a given network flow.

<pre>
<span style={{color: "white"}}>
<span style={{color: "#d2a8ff"}}>community_id</span>(src_ip<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>ip</span>, dst_ip<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>ip</span>, proto<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>str</span>, [src_port<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>int</span>, dst_port<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>int</span>, seed<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>int</span>]) <span style={{color: "#ff7b72"}}>-&gt;</span> <span style={{color: "#ffa657"}}>str</span>
</span>
</pre>

<!-- <pre>
<span style={{color: "white"}}>
<span style={{color: "#d2a8ff"}}>community_id</span>(src_ip<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>:ip</span>, src_port<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>:int</span>, dst_ip<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>:ip</span>, dst_port<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>:int</span>, port<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>:str</span>, seed<span style={{color: "#ff7b72"}}>=</span><span style={{color: "#ffa657"}}>:int</span>) -&gt; <span style={{color: "#ffa657"}}>str</span>
</span>
</pre> -->

### Description

The `src_ip` and `dst_ip` parameters are required. The `proto` string is also required and must be `tcp`, `udp`, `icmp` or `icmp6`. `src_port` and `dst_port` may only be specified if the other one is. `seed` can be used to set the initial hashing seed.

### Examples

```tql
from {
  source_ip: 1.2.3.4,
  source_port: 4584,
  destination_ip: 43.3.132.3,
  destination_port: 3483,
  protocol: "tcp",
}
cid = community_id(
  src_ip=source_ip,
  src_port=source_port,
  dst_ip=destination_ip,
  dst_port=destination_port,
  proto=protocol,
)
// cid == "1:koNcqhFRD5kb254ZrLsdv630jCM="
```

```tql
from {
  source_ip: 1.2.3.4,
  destination_ip: 43.3.132.3,
}
cid = community_id(src_ip=source_ip, dst_ip=destination_ip, proto="udp")
// cid == "1:7TrrMeH98PrUKC0ySu3RNmpUr48="
```
