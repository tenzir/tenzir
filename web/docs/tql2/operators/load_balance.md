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
let $cfg = [192.168.0.30, 192.168.0.31]

subscribe "input"
load_balance $cfg {
  let $endpoint = str($cfg) + ":8080"
  to_splunk $endpoint, hec_token="example-token-1234"
}
```
