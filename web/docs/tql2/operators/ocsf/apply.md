# apply

????????

```tql
ocsf::apply
```

## Current State

- Look at `class_uid` and `metadata.version` in incoming events (events without are dropped)
- Based on that, cast to the corresponding Tenzir type[^1] while warning about dropping extra fields or fields with incompatible types
- Use schema names such as `ocsf.network_activity`, no matter the OCSF version

[^1]: The OCSF types are slightly adjusted. For example, we currently use `time` for `timestamp_t`, which means that a `write_json` afterwards will not produce a number but a string. Is this what we want? Also, we turn `json_t` and bare `object`s into `string`, and use their JSON representation, even though some downstream tools might expect objects for them. Finally, we also break recursion, which to some degree is needed if we want to get a single type out at the end.

## Future Work

- Have options like `ocsf::apply activity_name="Network Activity"` to fill things in without needing them to be in the event
- Inspect `metadata.profiles` and adapt the schema accordingly. Current implementation assumes that no profiles are set. Or we could accept an argument for the list of profiles so that events with different profiles still get the same schema.
- Allow dropping all optional fields, or all optional and recommended fields. However, Tobias mentioned that a more fine-grained approach could be beneficial, so that we can keep some optional fields.
- Auto-assigning enums like `activity_name` based on `activity_id`, or automatically computing `type_uid`
- Casting for OCSF objects (as opposed to OCSF events) - should maybe be a function/keyword instead?)
- Validation (required fields and other semantic requirements)
