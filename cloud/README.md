# Tenzir cloud deployment

Deploy Tenzir easily to AWS with this set of terraform scripts.

You need Terraform>=0.12 and the AWS CLI V2 to be installed.

Currently supported:
- deploy VAST task definition to AWS Fargate with `make deploy`
- start a VAST task using `make start-task`
- stop all tasks using `make stop-all-tasks`

Note: to avoid typing vpc_id, subnet_cidr and aws_region each time you can set them up once and for all in a file called `default.env` that is included by the Makefile.

Caveats:
- The VAST node uses a public ip to bootstrap itself
- There is no way to connect to the VAST instance appart from spawning a bastion in the same network with the same security group
- Only local ephemeral storage is supported
