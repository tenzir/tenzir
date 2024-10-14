# drop

Removes fields from the event.

```tql
drop field...
```

### Description

Removes the given fields from the events.

### `field`

Field to be discarded from the event. Issues a warning if the field is not
present.

### Examples

```tql
from {
  name: "John",
  role: "Admin",
  info: {
    id: "cR32kdMD9",
    rank: 8411,
  },
}
drop name, info.id
write_json
```

```json title="Output"
{
  "role": "Admin",
  "info": {
    "rank": 8411
  }
}
```
