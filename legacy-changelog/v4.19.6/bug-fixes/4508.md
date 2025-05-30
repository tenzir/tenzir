We fixed a bug in the `shell` operator that could cause the process to crash
when breaking its pipe. Now, the operator shuts down with an error diagnostic
instead.

Pipelines with the `python` operator now deploy more quickly, as their
deployment no longer waits for the virtual environment to be set up
successfully.
