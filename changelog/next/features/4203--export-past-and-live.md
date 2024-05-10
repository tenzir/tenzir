* The `export` operators now features a `--retro`
flag. This flag will make the operators export past events, even when `--live`
is set:
  * Specifying *both* `--retro --live` will first export all past events and
  then go into live mode.
  * Specifying *only* `--live`  behaves like it did previously. It only exports
  new events.
  * Specifying *only* `--retro` behaves as if the flag was not set, just like
  previously without any flag.
  * Specifying none of the flags is also unchanged.
* The `diagnostics` and `metrics` operators also feature these flags, with the
same behavior.
