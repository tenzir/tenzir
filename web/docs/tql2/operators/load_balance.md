# load_balance

Routes the data through alternating subpipelines.

```tql
load_balance over:list { … }
```

## Description

TODO

### `over: list`

TODO

### `{ … }`

TODO

## Examples

### Route data to multiple TCP ports

```tql
let $cfg = ["192.168.0.30:8080", "192.168.0.30:8081"]

subscribe "input"
load_balance $cfg {
  write_json
  save $cfg
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
