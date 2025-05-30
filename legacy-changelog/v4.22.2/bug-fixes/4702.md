We fixed a bug in the `buffer` operator that caused it to break when
restarting a pipeline or using multiple buffers in a "parallel" context,
such as in `load_tcp`'s pipeline argument.
