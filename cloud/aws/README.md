# VAST AWS deployment

*Disclaimer: the native cloud architecture is still highly experimental and
subject to change without notice. Do not use in production yet.*

Running VAST on AWS relies on two serverless building blocks:

1. [Fargate](https://aws.amazon.com/fargate/) for VAST server nodes
2. [Lambda](https://aws.amazon.com/lambda/) as compute service to perform tasks,
   such as executing queries and ingesting data ad hoc

For storage on the VAST server in Fargate, VAST uses
[EFS](https://aws.amazon.com/efs/) as a drop-in network filesystem. It provides
persistence of the database files across task executions with a reasonable
throughput-latency trade-off.

The sketch below illustrates the high-level architecture:

![AWS
Architecture](https://user-images.githubusercontent.com/53797/157068659-41d7c9fe-8403-40d0-9cdd-dae66f0bf62e.png)

### Usage

We provide a CLI powered by [PyInvoke](cli.py) that wraps
[Terraform](https://www.terraform.io/) and `aws` CLI commands for a streamlined
UX. You can list the available higher level commands with `./vast-cloud --list`.

We provide a Docker executor to package all the dependencies required to run the
CLI. The [`./vast-cloud`](vast-cloud) executable script automatically builds and
runs the [pre-configured image](docker/cli.Dockerfile) for you.

The CLI looks for a `.env` file in the current directory. That file, together
with the environment variables, must define the following:

- `VAST_AWS_REGION`: the region of the VPC where to deploy Fargate and Lambda
  resources.

- `VAST_PEERED_VPC_ID`: an existing VPC to which you plan to attach your VAST stack.
  You can use `aws ec2 describe-vpcs --region $region` to list available VPCs.
  The deployment script will create a new VPC and peer it to the existing one.

- `VAST_CIDR`: the IP range where the VAST stack will be placed. Terraform will
  create a new VPC with this CIDR, so it should not overlapp with any of your
  existing VPCs.

Optionally, you can also define the following variables:

- `VAST_VERSION`: the version of VAST that should be used. By default it is set
  to the latest release. Version should be `v1.1.0` or higher. You can also use
  the latest commit on the main branch by specifying `latest`.

- `VAST_SERVER_STORAGE_TYPE`: the type of volume to use for the VAST server. Can
  be set to either
  - EFS (default) -> persistent accross task execution, infinitely scalable, but
    higher latency.
  - ATTACHED -> the local storage that comes by default with Fargate tasks. It
    is lost when the task is stopped.

Here's an example:

```bash
VAST_PEERED_VPC_ID=vpc-059a7ec8aac174fc9
VAST_CIDR=172.31.48.0/24
VAST_AWS_REGION=eu-north-1
```

Next, we take a look at some use cases. To setup Fargate and the Lambda
function, from within this folder use `./vast-cloud deploy`. You can tear down
the resources using the dual command, `./vast-cloud destroy`. See `./vast-cloud`
for a brief description of available commands.

Caveats:

- Access to the VAST server is enforced by limiting inbound traffic to its local
  private subnet.

- A NAT Gateway is created automatically, you cannot specify an existing one. It
  will be billed at [an hourly rate](https://aws.amazon.com/vpc/pricing/) even
  when you aren't running any workload, until you tear down the entire stack.

- You might sometime bumb into _error waiting for Lambda Function creation:
  InsufficientRolePermissions_ while deploying the stack. You can usually solve
  this by running `./vast-cloud deploy` again a few minutes later.


#### Start a VAST server (Fargate)

To deploy a VAST server as Fargate task, run:

```bash
./vast-cloud start-vast-server
```

This launches the official `tenzir/vast` Docker image, which executes the
command `vast start`.

You can replace the running task by a new one with:
```bash
./vast-cloud restart-vast-server
```

**Notes**: 
- if you use `ATTACHED` for the storage type, restarting the server task will
  empty the database.
- multiple invocations of `./vast-cloud run-vast-task` create multiple Fargate
  tasks, which prevents other commands from working correctly. We recommand
  using exclusively `start-vast-server` and `restart-vast-server`.

#### Run a VAST client on Fargate

You can run VAST client commands from within the Fargate server task using:

```bash
./vast-cloud execute-command --cmd "vast status"
```

This uses ECS Exec to connect to the container. The Fargate task should be in
`RUNNING` state and you sometime need a few extra seconds for the ECS Exec agent
to start.

If you do not specify the `cmd` option, it will start an interactive bash shell.
This comes handy to inspect the server environment and check whether things are
up and running.

#### Run a VAST client on Lambda

To run a VAST client from Lambda, use the `run-lambda` target:

```bash
./vast-cloud run-lambda --cmd "vast status"
```

The Lambda image also contains extra tooling, such as the AWS CLI, which is
useful to run batch imports or exports to other AWS services.


#### Shutdown

To shutdown all Fargate resources, run:

```bash
./vast-cloud stop-all-tasks
```

## Architecture

The AWS architecture builds on serverless principles to deliver a scalable
cloud-native deployment option. To combine continuously running services with
dynamic ad-hoc tasks, we employ Lambda and Fargate as building blocks for
on-demand query capacity while continuously ingesting data.

Specifically, we embed the long-running VAST server in a Fargate task
definition, which allows for flexible resource allocation based on compute
resource needs. VAST mounts EFS storage for maximum flexibility and
pay-as-you-go scaling.

The VAST client typically performs short-running ad-hoc tasks, like ingesting a
file or running query. We map such actions to Lambda functions.

### VPC Infrastructure

The provided Terraform script creates the following architecture within a given
VPC:

![VAST VPC
Architecture](https://user-images.githubusercontent.com/53797/157026500-8845d8bc-59cf-4de2-881e-e82fbd84da26.png)

The assumption is that the VPC has an Internet Gateway attached. Given a CIDR
block within this VPC, Terraform creates two subnets:

1. **VAST Subnet**: a private subnet where the VAST nodes and other security
   tools run.
2. **Gateway Subnet**: a public subnet to talk to other AWS services and the
   Internet

### Images and Registries

Both Lambda and Fargate deploy VAST as a Docker image. Fargate runs the official
[tenzir/vast](https://hub.docker.com/r/tenzir/vast) image. Lambda imposes two
additional requirements:

1. The image must contain the Lambda Runtime Interface
2. ECR must host the image in the region where the Lambda is deployed

For that reason, our toolchain builds a Lambda-specific image locally and pushes
it to a private ECR repository.

![Docker
workflow](https://user-images.githubusercontent.com/53797/157065561-82cf8bc6-b314-4439-b66f-c8e3a93e431b.png)
