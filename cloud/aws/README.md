# VAST AWS deployment

*Disclaimer: the native cloud architecture is still highly experimental and
subject to change without notice. Do not use in production yet.*

Running VAST on AWS relies on two serverless building blocks:

1. [Fargate](https://aws.amazon.com/fargate/) for VAST server nodes
2. [Lambda](https://aws.amazon.com/lambda/) as compute service to perform
   tasks, such as executing queries and ingesting data ad hoc

For storage on the VAST server in Fargate, VAST uses
[EFS](https://aws.amazon.com/efs/) as a drop-in network filesystem. It provides
persistence of the database files across task executions with a reasonable
throughput-latency trade-off.

The sketch below illustrates the high-level architecture:

![AWS Architecture](https://user-images.githubusercontent.com/53797/157068659-41d7c9fe-8403-40d0-9cdd-dae66f0bf62e.png)

### Usage

The provided [Makefile](Makefile) wraps [Terraform](https://www.terraform.io/)
and `aws` CLI commands for a streamlined UX. You need the following tools
installed locally:

- Terraform (> v1): `terraform` to provision the infrastructure
- AWS CLI (v2): `aws` (plus `SessionManager`) to automate AWS interaction
- Docker: `docker` to build a container for Lambda
- `jq` for wrangling JSON output

To avoid having to provide required Terraform variables manually, the Makefile
looks for a file `default.env` in the current directory. It must define the
following variables:

- `aws_region`: the region of the VPC where to deploy Fargate and Lambda
  resources.

- `vpc_id`: an existing VPC to which you plan to attach your VAST stack. You
  can use `aws ec2 describe-vpcs --region $region` to list available VPCs.

- `subnet_cidr`: the subnet *within* the VPC where the VAST stack will be
  placed. Terraform will create this subnet and it must not overlap with an
  existing subnet in this VPC and VPCs peered to it. You can use
  `aws ec2 describe-subnets --region $region` to list existing subnets to pick
  a non-overlapping one within the VPC subnet.

Here's an example:

```bash
vpc_id=vpc-059a7ec8aac174fc9
subnet_cidr=172.31.48.0/24
aws_region=eu-north-1
```

Next, we take a look at some use cases. To setup Fargate and one Lambda
instance, use `make deploy`. You can tear down the resources using the dual
command, `make destroy`. See `make help` for a brief description of available
commands.

Caveats:

- Access to the VAST server is enforced by limiting inbound traffic to its
  local private subnet.

- A NAT Gateway is created automatically, you cannot specify an existing one.
  It will be billed at [an hourly rate](https://aws.amazon.com/vpc/pricing/)
  even when you aren't running any workload, until you tear down the entire
  stack.


#### Start a VAST server (Fargate)

To deploy a VAST server as Fargate task, run:

```bash
make run-task
```

This launches the official `tenzir/vast` Docker image, which executes the
command `vast start`. You can get a shell via ECS Exec to the image where the
VAST server runs with `make execute-command`, e.g., to run `vast status` to see
whether things are up and running.

**Caveat**: in the current setup, multiple invocations of `make run-task`
create multiple Fargate tasks, which prevents other Makefile targets from
working correctly.

**Note**: the Fargate container currently uses local storage only.

#### Run a VAST client (Lambda)

To run a VAST in Lambda, e.g., `vast status`, use the `run-lambda` target:

```bash
make run-lambda CMD="vast status"
```


#### Shutdown

To shutdown all Fargate resources, run:

```bash
make stop-all-tasks
```

## Architecture

The AWS architecture builds on serverless principles to deliver a scalable
cloud-native deployment option. To combine continuously running services with
dynamic ad-hoc tasks, we employ Lambda and Fargate as building blocks for
on-demand query capacity while continuously ingesting data.

Specifically, we embed the long-running VAST server in a Fargate task
definition, which allows for flexible resource allocation based on
compute resource needs. VAST mounts EFS storage for maximum flexibility and
pay-as-you-go scaling.

The VAST client typically performs short-running ad-hoc tasks, like ingesting
a file or running query. We map such actions to Lambda functions.

### VPC Infrastructure

The provided Terraform script creates the following architecture within a given
VPC:

![VAST VPC Architecture](https://user-images.githubusercontent.com/53797/157026500-8845d8bc-59cf-4de2-881e-e82fbd84da26.png)

The assumption is that the VPC has an Internet Gateway attached. Given a CIDR
block within this VPC, Terraform creates two subnets:

1. **VAST Subnet**: a private subnet where the VAST nodes and other security
   tools run.
2. **Gateway Subnet**: a public subnet to talk to other AWS services and the
   Internet

### Images and Registries

Both Lambda and Fargate deploy VAST as a Docker image. Fargate runs the
official [tenzir/vast](https://hub.docker.com/r/tenzir/vast) image. Lambda
imposes two additional requirements:

1. The image must contain the Lambda Runtime Interface
2. ECR must host the image in the region where the Lambda is deployed

For that reason, our toolchain builds a Lambda-specific image locally and
pushes it to a private ECR repository.

![Docker workflow](https://user-images.githubusercontent.com/53797/157065561-82cf8bc6-b314-4439-b66f-c8e3a93e431b.png)
