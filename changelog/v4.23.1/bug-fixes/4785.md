We fixed a bug in TQL2's `where` operator that made it sometimes return
incorrect results for events for which the predicate evaluated to `null`. Now,
the operator consistently warns when this happens and drops the events.
