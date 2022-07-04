# AWS

## Architecture Choices

The AWS architecture builds on serverless principles to deliver a scalable
cloud-native deployment option. To combine continuously running services with
dynamic ad-hoc tasks, we use Lambda and Fargate as building blocks for on-demand
query capacity while continuously ingesting data.

Specifically, we embed the long-running VAST server in a Fargate task
definition, which allows for flexible resource allocation based on compute
resource needs. VAST mounts EFS storage for maximum flexibility and
pay-as-you-go scaling. The VAST client performs short-running ad-hoc tasks, like
ingesting a file or running query. We map such actions to Lambda functions.

## VPC Infrastructure

The provided Terraform script creates the following architecture within a given
VPC:

![VAST VPC Architecture](https://user-images.githubusercontent.com/7913347/177141492-b99cce77-3c10-4740-bbdc-4fc2f43b8abc.png)

The assumption is that the VPC has an Internet Gateway attached. Given a CIDR
block within this VPC, Terraform creates two subnets:

1. **VAST Subnet**: a private subnet where the VAST nodes and other security
   tools run.
2. **Gateway Subnet**: a public subnet to talk to other AWS services and the
   Internet

To resolve the IP address of the VAST server and other appliances, we use AWS
Cloud Map as a service discovery provider.

## Images and Registries

Both Lambda and Fargate deploy VAST as a Docker image. Fargate runs the official
[tenzir/vast](https://hub.docker.com/r/tenzir/vast) image. Lambda imposes two
additional requirements:

1. The image must contain the Lambda Runtime Interface
2. ECR must host the image in the region where the Lambda is deployed

For that reason, our toolchain builds a Lambda-specific image locally and pushes
it to a private ECR repository.

![Docker
workflow](https://user-images.githubusercontent.com/53797/157065561-82cf8bc6-b314-4439-b66f-c8e3a93e431b.png)
