# Tenzir cloud deployment

Deploy VAST easily to AWS with Terraform.

## Requirements

You need to have installed locally:
- Terraform>=1 
- the AWS CLI V2
- jq for parsing AWS CLI results

Terraform variables to provide:
- `vpc_id`: the existing VPC to which you plan to attach your VAST stack
- `subnet_cidr`: the ip range of the subnet where the VAST stack will be placed. A subnet with that range will be created. It should not overlap with an existing subnet in specified VPC and the VPCs peered to it.
- `aws_region`: the region in which this VPC is defined

## Features

Currently supported:
- deploy VAST both as a Fargate task definition and a Lambda function with `make deploy`
- start a VAST server task using `make start-task`
- connect to the VAST server through a VAST client running on aws lambda:
  - `make run-lambda CMD="vast status"`
- connect directly to the VAST server through ECS Exec with `make execute-command`
- stop all tasks using `make stop-all-tasks`
- run `make help` for some basic inline documentation.

Note: to avoid having to specify `vpc_id`, `subnet_cidr` and `aws_region` with each `make` call, you can set them up once and for all in a file called `default.env` (which will be included by the Makefile).

Caveats:
- Only local ephemeral storage is supported for now
- `get-task-ip`, `run-lambda` and `execute-command` will work properly only if you have one and only one task running on Fargate
- Access to the VAST server is enforced by limiting inbound trafic to its local private subnet
- Currently the AZ for the IDS appliance stack cannot be specified
- A NAT Gateway is created automatically, you cannot specify an existing one
