# every

Runs a pipeline periodically at a fixed interval.

```tql
every interval:duration { … }
```

## Description

The `every` operator repeats running a pipeline indefinitely at a fixed interval. The first run is starts directly when the outer pipeline itself starts. After every interval, the previous run is finished by stopping its input, and a new run is started.

## Examples

### Produce one event per second and enumerate the result

```tql
every 1s {
  from {}
}
enumerate
```

```tql
{"#": 0} // immediately
{"#": 1} // after 1s
{"#": 2} // after 2s
{"#": 3} // after 3s
// … continues like this
```

### Fetch the results from an API every 10 minutes

```tql
every 10min {
  load_http "example.org/api/threats"
  read_json
}
publish "threat-feed"
```
