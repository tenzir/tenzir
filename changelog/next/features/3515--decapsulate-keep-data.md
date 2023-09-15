The `decapsulate` operator no longer drops the data field in incoming packet
events. To restore the old behavior, use `decapsulate | drop data` explicitly.
