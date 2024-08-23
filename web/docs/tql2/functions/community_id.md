# community_id

```c
community_id(src_ip=<ip>, src_port=<int>, dst_ip=<ip>, dst_port=<int>, proto=<string>)
community_id(src_ip=ip, dst_ip=ip, proto=string, src_port=int, dst_port=int, seed=int)
community_id(src_ip=<ip>, dst_ip=<ip>, proto=<string>, src_port=<int>, dst_port=<int>, seed=<int>)
```

<pre>
community_id(src_ip=ip, dst_ip=ip, proto=string, src_port=int, dst_port=int, seed=int)
</pre>

Brief description.


### Parameters


### Examples

~~~
from {
  SourceIp: 1.2.3.4,
  SourcePort: 4584,
  DestinationIp: 43.3.132.3,
  DestinationPort: 34831,
  proto: "tcp"
}
CommunityID = community_id(
  src_ip=SourceIp,
  src_port=SourcePort,
  dst_ip=DestinationIp,
  dst_port=DestinationPort,
  proto=Protocol,
)

~~~
