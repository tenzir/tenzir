We now gracefully handle a panic in `write_syslog`, when `structured_data` does
not have the expected shape.
