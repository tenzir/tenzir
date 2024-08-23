---
sidebar_position: 4
---

# User-Defined Operators

~~~
def export_last($t: duration) {
  export
  where meta.import_time > now() - $t
}

export_last 1h
~~~
