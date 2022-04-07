Trivial queries with large results set sometimes resulted in a non-zero exit
code with a "remote link unreachable" error, dropping some events. VAST now
properly shuts down exports only after all events were delivered.
