# VAST cloud deployment

Deploy VAST easily to AWS with Terraform.

## Proposed architecture

### Infrastructure overview

This script creates the networking infrastructure and the compute resources that VAST will run on.

In terms of networking, we make the assumption that the user has a VPC with some appliances that he wishes to monitor. This VPC has a CIDR block of at least 32 IPs that is not yet assigned to any subnet and it also has an Internet Gateway attached.

The script takes the VPC ID and the CIDR block as input and from that it creates:
- 2 subnets spanning that CIDR block:
  - a public one where it places a NAT Gateway so that the VAST instances can communicate with the AWS APIs and if required, the internet
  - a private one with the tooling resources themselves
- the configurations to run VAST either on Fargate as a Server or on Lambda as a client.

<p align="center">
<img src="https://user-images.githubusercontent.com/7913347/155995627-cb25056e-2c6d-49f9-a55a-e8dc5a90f28a.svg" width="90%">
</p>

### Images and registries

Both on Lambda and Fargate, VAST is deployed as a Docker image. Fargate runs the official [tenzir/vast](https://hub.docker.com/r/tenzir/vast) image. To run VAST on AWS Lambda, we need:
- an extra image layer containing the Lambda Runtime Interface
- the image to be hosted on ECR in the region where the Lambda is deployed is deployed

For that reason, when deploying VAST to AWS, the user will build the Lambda specific Docker image locally and push it to a private ECR repository created by the Terraform deployment script itself.

<p align="center">
<img src="https://user-images.githubusercontent.com/7913347/156000070-c9857869-7621-4e95-a517-b4e065b36ed3.svg" width="70%">
</p>

### VAST processes

The VAST server is a long running process. We decided to use Fargate as the cloud resource to run it because it is simple, flexible and very well suited for this usecase.

To interact with the VAST server you need a VAST client. The user can run VAST commands:
- directly within the Fargate container using `make execute-command`. In that case he will be limited by the tooling available in the image
- from AWS lambda with `make run-lambda`, which makes it easier to integrate new tooling or allow connections to other AWS services

<p align="center">
<img src="https://user-images.githubusercontent.com/7913347/156000469-a0f8b519-64c1-43ec-91dc-1339b41f90be.svg" width="70%">
</p>

## Requirements

To run the provided tooling, you need to have installed locally:
- Terraform version>=1 
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

## Caveats
- Only local ephemeral storage is supported for now
- `get-task-ip`, `run-lambda` and `execute-command` will work properly only if you have one and only one task running on Fargate
- Access to the VAST server is enforced by limiting inbound traffic to its local private subnet
- Currently the AZ for the IDS appliance stack cannot be specified
- A NAT Gateway is created automatically, you cannot specify an existing one
