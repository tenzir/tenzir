# Docker definitions

We use docker-compose to forward the build variables and specify the right
context of the images required by each infrastructure modules. The command
`./vast-cloud build-image` will use the specified `docker-compose.yml` to
bootstrap the builds and pushes.

**Warning:** You should avoid re-using service names accross modules, otherwise
images stored in the remote repository will conflict.
