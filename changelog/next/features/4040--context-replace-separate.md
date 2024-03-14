The `--replace` option for the `enrich` operator causes the input values to be
replaced with their context instead of extending the event with the context,
resulting in a leaner output.

The `--separate` option makes the `enrich` and `lookup` operators handle each
field individually, duplicating the event for each relevant field, and
returning at most one context per output event.
