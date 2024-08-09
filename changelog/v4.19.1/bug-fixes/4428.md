Activating heartbeats via `-X`/`--set` on an `amqp` saver triggered socket errors
if the interval between sent messages was larger than the heartbeat interval.
This has been fixed by handling heartbeat communication correctly in such cases.
