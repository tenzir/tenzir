We fixed a bug that caused the `context_updates` field in `metrics lookup` to be
reported once per field specified in the corresponding `lookup` operator instead
of being reported once per operator in total.
