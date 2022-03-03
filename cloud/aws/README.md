# Tenzir cloud deployment

Deploy Tenzir easily to AWS with this set of terraform scripts.

You need Terraform>=1 and the AWS CLI V2 to be installed.

Terraform variables to provide:
- vpc_id: the existing VPC to which you plan to attach your VAST stack
- subnet_cidr: the ip range of the subnet where the VAST stack will be placed. A subnet with that range will be created, so it should not overlap with an existing subnet.
- aws_region: the region in which this VPC is defined

Currently supported:
- deploy VAST both as a Fargate task definition and a Lambda function with `make deploy`
- start a VAST server task using `make start-task`
- stop all tasks using `make stop-all-tasks`

Note: to avoid typing vpc_id, subnet_cidr and aws_region each time you can set them up once and for all in a file called `default.env` that is included by the Makefile.

Caveats:
- The VAST node uses a public ip to bootstrap itself
- You need to get the ip address of the vast server manually and connect to it using something like `make run-lambda CMD="vast -e 172.31.56.179:42000 version`
- Only local ephemeral storage is supported
- Access to the VAST server is enforced by limiting inbound trafic to its local subnet only
