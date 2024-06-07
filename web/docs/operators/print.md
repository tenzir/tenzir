---
sidebar_custom_props:
  operator:
    transformation: true
---

# print

Prints the specified record field as a string.

## Synopsis

```
print <input> <printer> <args>...
```

## Description

The `print` operator prints a given `<input>` field of type `record` using `<printer>` 
and replaces this field with the result. 

### `<input>`

Specifies the field of interest. The field must be a record type.

### `<printer>` `<args>`

Specifies the printer format and the corresponding arguments specific to each printer. 
`<printer>` can be one of the following` text-based printers in [formats](../formats.md): 
json, lines, xsv, yaml, zeek-tsv.

## Examples

Print [JSON](../formats/json.md) from the `flow` field stored in `suricata.json`, 
returning the result from CSV printer as the new `flow` field.

```
from suricata.json read json | print flow csv
```

The transformation looks like this:

```json
{
  "timestamp": "2021-11-17T13:32:43.237882",
  "flow_id": 852833247340038,
  "flow": {
    "pkts_toserver": 1,
    "pkts_toclient": 0,
    "bytes_toserver": 54,
    "bytes_toclient": 0,
    "start": "2021-11-18T01:11:55.722853",
    "end": "2021-11-18T01:11:55.722853",
    "age": 0,
    "state": "new",
    "reason": "timeout",
    "alerted": false,
    "wrong_thread": null,
    "bypass": null,
    "bypassed": null,
    "tcp": null,
    "emergency": null
 }
}
```

```json
{
  "timestamp": "2021-11-17T13:32:43.237882",
  "flow_id": 852833247340038,
  "flow": "pkts_toserver,pkts_toclient,bytes_toserver,bytes_toclient,start,end,age,
  state,reason,alerted,wrong_thread,bypass,bypassed,tcp,emergency\n1,0,54,0,2021-11-18T01:11:
  55.722853,2021-11-18T01:11:55.722853,0,new,timeout,false,,,,,\n"
}
```
