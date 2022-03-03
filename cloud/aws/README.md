# Tenzir cloud deployment

Deploy Tenzir easily to AWS with this set of terraform scripts.

You need to have installed locally:
- Terraform>=1 
- the AWS CLI V2
- jq for parsing AWS CLI commands

Terraform variables to provide:
- vpc_id: the existing VPC to which you plan to attach your VAST stack
- subnet_cidr: the ip range of the subnet where the VAST stack will be placed. A subnet with that range will be created, so it should not overlap with an existing subnet.
- aws_region: the region in which this VPC is defined

Currently supported:
- deploy VAST both as a Fargate task definition and a Lambda function with `make deploy`
- start a VAST server task using `make start-task`
- connect to the VAST server through a VAST client running on aws lambda:
  - `make run-lambda CMD="vast version"`
- stop all tasks using `make stop-all-tasks`

Note: to avoid typing vpc_id, subnet_cidr and aws_region each time you can set them up once and for all in a file called `default.env` that is included by the Makefile.

Caveats:
- The VAST node uses a public ip to bootstrap itself
- Only local ephemeral storage is supported
- The `get-task-ip` and `run-lambda` commands will work properly if you have only one task running on Fargate
- Access to the VAST server is enforced by limiting inbound trafic to its local subnet only
