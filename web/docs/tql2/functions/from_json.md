---
sidebar_class_name: hidden
---

# from_json

```
<string>.from_json(
  schema=null,
  selector=null,
  unnest=null,
  many=false,
)
```

### Examples

```
"null".from_json() == null
"[1, 2, 3]".from_json() == [1, 2, 3]
"{x: 42}".from_json() == {x: 42}

// TODO
"{x: 42}{y: 43}".from_json(many=true) == [{x: 42}{y: 43}]
```
