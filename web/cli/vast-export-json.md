The JSON export format renders events in newline-delimited JSON (aka.
[JSONL](https://en.wikipedia.org/wiki/JSON_streaming#Line-delimited_JSON)).

By default, durations render as human-readable strings with up to two decimal
places and a unit, e.g., `2.31s` or `24.1ms`. The option `--numeric-durations`
causes VAST to render durations as numbers instead, representing the time in
nanoseconds resolution.
