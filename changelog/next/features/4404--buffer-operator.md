The `buffer` operator buffers up to the specified number of events in an
in-memory buffer. By default, operators in a pipeline run only when their
downstream operators want to receive input. This mechanism is called back
pressure. The `buffer` operator effectively breaks back pressure by storing up
to the specified number of events in memory, always requesting more input, which
allows upstream operators to run uninterruptedly even in case the downstream
operators of the buffer are unable to keep up. This allows pipelines to handle
data spikes more easily.
