We fixed a problem with the TCP connector that caused pipeline restarts on the
same port to fail if running `shell` or `python` operators were present.
