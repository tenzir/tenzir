# community_id

Computes the [Community ID](https://github.com/corelight/community-id-spec) for
a network connection/flow.

```tql title="Synopsis"
community_id(src_ip=ip, dst_ip=ip, proto=str,
             [src_port=int, dst_port=int, seed=int]) -> str
```

## Description

The `community_id` function computes a unique hash digest of a network
connection according to the [Community
ID](https://github.com/corelight/community-id-spec)
spec. The digest is useful for pivoting between multiple events that belong to
the same connection.

The `src_ip` and `dst_ip` parameters are required. The `proto` string is also required and must be `tcp`, `udp`, `icmp` or `icmp6`. `src_port` and `dst_port` may only be specified if the other one is. `seed` can be used to set the initial hashing seed.

## Examples

### Compute a Community ID from a flow 5-tuple

```tql title="Pipeline"
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
select cid
```

```tql title="Output"
{
  "cid": "1:koNcqhFRD5kb254ZrLsdv630jCM="
}
```

### Compute a Community ID from a host pair

Because source and destination port are optional, it suffices to provide two IP
addreses to compute a valid Community ID.

```tql title="Pipeline"
from {
  src: 1.2.3.4,
  dst: 43.3.132.3,
}
cid = community_id(src_ip=src, dst_ip=dst, proto="udp")
select cid
```

```tql title="Output"
{
  "cid": "1:7TrrMeH98PrUKC0ySu3RNmpUr48="
}
```
