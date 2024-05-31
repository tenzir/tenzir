The `export` operators now features a `--retro` flag. This flag will make the
operators first export past events, even when `--live` is set. Specifying *both*
`--retro --live` will first export all past events and then go into live mode.
**Be aware that this is currently experimental and has a gap for events which
happen before switching to live mode.
The `diagnostics` and `metrics` operators also feature these flags, with the
same behavior.
