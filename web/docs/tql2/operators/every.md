# every

Runs a pipeline every time an `interval` elapses.

```tql
every interval:duration { … }
```

## Description

Runs a pipeline every time an `interval` elapses.

### `interval: duration`

The duration to wait between running pipeline.

### `{ … }`

The pipeline to execute.

## Examples

```tql
every 10 min {
  context update 
}
```

```tql
every 1h {
  load_http "url"
}
publish "threat-feed"
```
