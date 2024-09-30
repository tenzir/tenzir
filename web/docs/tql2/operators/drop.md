# drop

Removes fields from the event.

<pre>
<span style={{color: "white"}}>
<span style={{color: "#d2a8ff"}}>drop</span> fields...
</span>
</pre>


### Usage

... If a column does not exist, a warning will be issued instead.


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
// this == {
//   role: "Admin",
//   info: {
//     rank: 8411
//   },
// }
```

```
from { name: "John" }
drop address
// this == { name: "John" }
```
