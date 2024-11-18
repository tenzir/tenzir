# load_balance

Routes the data to one of multiple subpipelines.

```tql
load_balance over:list { … }
```

## Description

The `load_balance` operator spawns a nested pipeline for each element in the
given list. Incoming events are distributed to exactly one of the nested
pipelines. This operator may reorder the event stream.

### `over: list`

This must be a `$`-variable, previously declared with `let`. For example, to
load balance over a list of ports, use `let $cfg = [8080, 8081, 8082]` followed
by `load_balance $cfg { … }`.

### `{ … }`

The nested pipeline to spawn. This pipeline can use the same variable as passed
to `over`, which will be resolved to one of the list items. The following
example spawns three nested pipelines, where `$port` is bound to `8080`, `8081`
and `8082`, respectively.

```tql
let $cfg = [8080, 8081, 8082]
load_balance $cfg {
  let $port = $cfg
  // … continue here
}
```

The given subpipeline must end with a sink. This limitation might be removed in
future versions.

## Examples

### Route data to multiple TCP ports

```tql
let $cfg = ["192.168.0.30:8080", "192.168.0.30:8081"]

subscribe "input"
load_balance $cfg {
  write_json
  save_tcp $cfg
}
```

### Route data to multiple Splunk endpoints

```tql
let $cfg = [{
  ip: 192.168.0.30,
  token: "example-token-1234",
}, {
  ip: 192.168.0.31,
  token: "example-token-5678",
}]

subscribe "input"
load_balance $cfg {
  let $endpoint = str($cfg.ip) + ":8080"
  to_splunk $endpoint, hec_token=$cfg.token
}
```
