# drop

Removes columns from the event.

```
drop <selector>, ...
```

### Description


### Examples

```
from {
  name: "John",
  role: "Admin",
  info: {
    id: "cR32kdMD9",
    rank: 8411,
  },
}
drop name, info.id
---
{
  role: "Admin",
  info: {
    rank: 8411
  },
}
```

```
from { name: "John" }
drop address
---
warning: could not find field `address`
 --> <input>:1:16
  |
2 | drop address
  |      ~~~~~~~
  |
{ name: "John" }
```
