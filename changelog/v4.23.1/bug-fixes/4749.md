The `docker compose` setup now uses separate local volumes for each `tenzir` directory. This fixes a bug where restarting the container resets installed packages or pipelines.
