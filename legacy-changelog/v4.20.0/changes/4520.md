The `lines` printer now does not perform any escaping and is no longer an alias to
the `ssv` printer. Additionally, nulls are skipped, instead of being printed
as `-`.
