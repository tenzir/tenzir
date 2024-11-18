# drop

Removes fields from the event.

```tql
drop field...
```

## Description

Removes the given fields from the events. Issues a warning if a field is not
present.

## Examples

### Drop fields from the input

```tql
from {
  src: 192.168.0.4,
  dst: 192.168.0.31,
  role: "admin",
  info: {
    id: "cR32kdMD9",
    msg: 8411,
  },
}
drop role, info.id
```

```tql
{
  src: 192.168.0.4,
  dst: 192.168.0.31,
  info: {msg: 8411},
}
```
