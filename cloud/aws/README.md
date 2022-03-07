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

![AWS Architecture](https://user-images.githubusercontent.com/53797/156728636-9909c4aa-34a0-47f4-b6f0-50d7bfe96e28.png)

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

- `aws_region`: the region where to deploy the VPC

- `vpc_id`: an existing VPC to which you plan to attach your VAST stack. You
  can use `aws ec2 describe-vpcs --region $region` to list available VPCs.

- `subnet_cidr`: the subnet *within* the VPC where the VAST stack will be
  placed. Terraform will create a this subnet and it must not overlap with an
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

---------------------------------------------

WIP

---------------------------------------------


## Architecture

Lambda is the perfect abstraction for short-running one-shot operation, e.g.,
ingesting a file or executing a query.

### VPC Infrastructure

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
- the image to be hosted on ECR in the region where the Lambda is deployed

For that reason, when deploying VAST to AWS, the user will build the Lambda specific Docker image locally and push it to a private ECR repository created by the Terraform deployment script itself.

<p align="center">
<img src="https://user-images.githubusercontent.com/7913347/156000070-c9857869-7621-4e95-a517-b4e065b36ed3.svg" width="70%">
</p>

### VAST processes

The VAST server is a long running process. We decided to use Fargate as the cloud resource to run it because it is simple, flexible and very well suited for this usecase.

To interact with the VAST server you need a VAST client. The user can run VAST commands:


<p align="center">
<img src="https://user-images.githubusercontent.com/7913347/156000469-a0f8b519-64c1-43ec-91dc-1339b41f90be.svg" width="70%">
</p>

### Setup

#### macOS

```bash
brew install awscli jq
brew install --cask session-manager-plugin
```
