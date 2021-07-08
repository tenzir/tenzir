Particularly busy imports caused the shutdown of the server process to hang,
if the import processes were still running, or had not yet flushed all data.
The server now shuts down correctly in these cases.
