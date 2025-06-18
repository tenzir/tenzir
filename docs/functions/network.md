---
title: network
category: IP, Subnet
example: '10.0.0.0/8.network()'
---

Retrieves the network address of a subnet.

```tql
network(x:subnet) -> ip
```

## Description

The `network` function returns the network address of a subnet.

## Examples

### Get the network address of a subnet

```tql
from {subnet: 192.168.0.0/16}
select ip = subnet.network()
```

```tql
{ip: 192.168.0.0}
```
