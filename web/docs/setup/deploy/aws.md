---
sidebar_position: 2
---

# AWS

:::caution Experimental
Tenzir's native cloud architecture is still highly experimental and
subject to change without notice.
:::

Running Tenzir on AWS relies on two serverless building blocks:

1. [Fargate](https://aws.amazon.com/fargate/) for Tenzir server nodes
2. [Lambda](https://aws.amazon.com/lambda/) as compute service to perform tasks,
   such as executing queries and ingesting data ad hoc

For storage, Tenzir uses [EFS](https://aws.amazon.com/efs/). The sketch below
illustrates the high-level architecture:

![AWS Architecture](aws-architecture.excalidraw.svg)

## Create an environment file

:::info Source Code Required
Make sure you have [downloaded the Tenzir source code](../download.md)
and change into the directory
[cloud/aws](https://github.com/tenzir/tenzir/tree/main/cloud/aws) that contains
all deployment scripts.
:::

Prior to running the deployment scripts, you need to setup required envionment
variables in a `.env` in the `cloud/aws` directory:

- `TENZIR_AWS_REGION`: the region of the VPC where to deploy Fargate and Lambda
  resources.

- `TENZIR_PEERED_VPC_ID`: an existing VPC to which you plan to attach your
  Tenzir stack. You can use `aws ec2 describe-vpcs --region $region` to list
  available VPCs. The deployment script will create a new VPC and peer it to the
  existing one.

- `TENZIR_CIDR`: the IP range where the Tenzir stack will be placed. Terraform
  will create a new VPC with this CIDR, so it should not overlapp with any of
  your existing VPCs.

Optionally, you can also define the following variables:

- `TENZIR_VERSION`: the version of Tenzir that should be used. By default it is
  set to the image tagged as `latest`. Can be any tag available on the
  tenzir/tenzir Dockerhub as long as it is more recent than `v1.1.0`. You can
  also build from source using `build`.

- `TENZIR_STORAGE_TYPE`: the type of volume to use for the Tenzir node and other
  tasks where persistence is useful. Can be set to either:
  - `EFS` (default): persistent accross task execution, infinitely scalable, but
    higher latency.
  - `ATTACHED`: the local storage that comes by default with Fargate tasks, lost
    lost when the task is stopped.

Here's an example:

```bash
TENZIR_PEERED_VPC_ID=vpc-059a7ec8aac174fc9
TENZIR_CIDR=172.31.48.0/24
TENZIR_AWS_REGION=eu-north-1
```

## Spin up infrastructure with Terraform

Once you have a `.env` file set up, you can deploy to AWS. To spin up the AWS
infrastructure, we use [Terragrunt](https://terragrunt.gruntwork.io/), a thin
[Terraform](https://www.terraform.io/) wrapper to keep a DRY configuration.

In case you are not familiar using these tools, we package the toolchain in a
small [Docker image][tenzir-cloud-dockerfile], wrapped behind a tiny shell
script [`tenzir-cloud`][tenzir-cloud-script]. The script builds the Docker
container on first use. You can trigger a manual build as well:

```bash
TENZIRCLOUD_REBUILD=1 ./tenzir-cloud
```

With the toolchain Docker image in place, `tenzir-cloud` is now ready to execute
commands via `docker run` that transports the `.env` configuration to the main
script [`main.py`][main.py] driving the Terragrunt invocation. To see what
commands are available, run `./tenzir-cloud --list`.

To create the AWS services, run:

```bash
./tenzir-cloud deploy
```

To tear everything down, use:

```bash
./tenzir-cloud destroy
```

[tenzir-cloud-dockerfile]: https://github.com/tenzir/tenzir/blob/main/cloud/aws/docker/cli.Dockerfile
[tenzir-cloud-script]: https://github.com/tenzir/tenzir/blob/main/cloud/aws/tenzir-cloud
[core.py]: https://github.com/tenzir/tenzir/blob/main/cloud/aws/cli/core.py

:::warning Caveats
- Access to the Tenzir node is enforced by limiting inbound traffic to its local
  private subnet.
- A NAT Gateway and a network load balancer are created automatically, you
  cannot specify existing ones. They will be billed at [an hourly
  rate](https://aws.amazon.com/vpc/pricing/) even when you aren't running any
  workload, until you tear down the entire stack.
:::

:::tip Get modules back in sync
Terragrunt sometimes fails to detect when it must run `terraform init`. When
that happens, you can force an init on all modules using `./tenzir-cloud init`.
:::

### Start a Tenzir node (Fargate)

Now that the AWS infrastructure is in place, you start the containers. To start
a Tenzir server as Fargate task, run:

```bash
./tenzir-cloud tenzir.start-server
```

By default, this launches the official `tenzir/tenzir` Docker image and executes
the command `tenzir start`. To use the Tenzir Pro image, check out the [AWS
Pro](aws-pro.md) documentation.

Check the status of the running server with:

```bash
./tenzir-cloud tenzir.server-status
```

You can replace the running server with a new Fargate task:

```bash
./tenzir-cloud tenzir.restart-server
```

Finally, to avoid paying for the Fargate resource when you are not using Tenzir,
you can shut down the server:

```bash
./tenzir-cloud stop-tenzir-server
```

:::info
- If you use `ATTACHED` as storage type, restarting the server task will wipe
  the database.
:::

### Run a Tenzir client on Fargate

After your Tenzir node is up and running, you can start spawning clients.
The `tenzir.server-execute` target lifts Tenzir command into an ECS Exec
operation. For example, to execute `tenzirctl status`, run:

```bash
./tenzir-cloud tenzir.server-execute --cmd "tenzirctl status"
```

If you do not specify the `cmd` option, it will start an interactive bash shell.
This comes handy to inspect the server environment and check whether things are
up and running.

:::note
The Fargate task should be in `RUNNING` state and you sometime need a few extra
seconds for the ECS Exec agent to start.
:::

### Run a Tenzir client on Lambda

To run a Tenzir client from Lambda, use the `tenzir.lambda-client` target:

```bash
./tenzir-cloud tenzir.lambda-client --cmd "tenzirctl status"
```

The Lambda image also contains extra tooling, such as the AWS CLI, which is
useful to run batch imports or exports to other AWS services.

## Configure and deploy cloud plugins

You can set activate a number of "cloud plugins" using the `.env` config file:

```
TENZIR_CLOUD_PLUGINS = workbucket,tests
```

### Continuously load data from Cloudtrail

If you have Cloudtrail enabled and pushing data into a bucket that is located in
the same AWS account as your Tenzir deployment, you can deploy an optional module
that will stream all the new events arriving in that bucket to the Tenzir
instance. To achieve this, assuming that Tenzir is already deployed, configure
the following in the `.env` file:
- `TENZIR_CLOUD_PLUGINS`: add `cloudtrail` to the list of plugins
- `TENZIR_CLOUDTRAIL_BUCKET_NAME`: the name of the bucket where Cloudtrail is
  pushing its events
- `TENZIR_CLOUDTRAIL_BUCKET_REGION`: the region where that bucket is located

Then run:

```bash
./tenzir-cloud deploy
```

You should see new events flowing into Tenzir within a few minutes:

```bash
./tenzir-cloud tenzir.lambda-client -c "tenzirctl count '#schema==\"aws.cloudtrail\"'"
```

Running the global `./tenzir-cloud destroy` command will also destroy optional
modules such as the Cloudtrail datasource. If you want to destroy the Cloudtrail
datasource resources only, use:

```bash
./tenzir-cloud destroy --step cloudtrail
```

:::warning Caveats
- To get notified of new objects in the bucket, we use EventBridge
  notifications. These notifications are not disabled automatically on your
  bucket when the stack is destroyed to avoid interfering with your existing
  notification systems.
:::

### Run MISP

[MISP](https://www.misp-project.org/) is a popular community driven open source
thread intelligence platform. It integrates naturally with Tenzir. To deployed a
preconfigured instance:

- add `misp` to the list of plugins in `TENZIR_CLOUD_PLUGINS`
- deploy (or re-deploy) the Tenzir stack including MISP with `./tenzir-cloud
  deploy`
- start the MISP server with `./tenzir-cloud misp.start`, wait a few minutes, MISP
  is rather slow to start :-)
- open an ssh tunnel to your instance running `./tenzir-cloud misp.tunnel`
- you can now connect to MISP `localhost:8080` on you local browser
  - Default login: `demo@tenzir.com` / `demo`
  - Default API KEY: `demodemodemodemodemodemodemodemodemodemo`
- you can also subscribe to MISP's ZeroMQ feed on `localhost:50000`

### Expose services publicly

We offer the capability to expose services securely using
Cloudflare Access. You need a Cloudflare account with a [zone
configured](https://developers.cloudflare.com/dns/zone-setups/full-setup/setup/)
to use this plugin.

- add `cloudflare` to the list of plugins in `TENZIR_CLOUD_PLUGINS`
- configure the environment:
  - `TENZIR_CLOUDFLARE_ACCOUNT_ID`: the account ID is provided in the overview
    page of you zone
  - `TENZIR_CLOUDFLARE_ZONE`: the domain of you zone, e.g example.com
  - `TENZIR_CLOUDFLARE_API_TOKEN`: an API token with
    - account permissions: `Cloudflare Tunnel:Edit`, `Access: Organizations,
      Identity Providers, and Groups:Edit`, `Access: Apps and Policies:Edit`
    - zone permissions:  `Access: Apps and Policies:Edit`, `DNS:Edit`
  - `TENZIR_CLOUDFLARE_EXPOSE`: comma separated list of output variables
    containing the local service URLs. Currently only `misp.ui_url` is supported
  - `TENZIR_CLOUDFLARE_AUTHORIZED_EMAILS`: comma seprated list of email addresses
    we want to authorize on the demo apps. Users will get access using [1 time
    PIN login](https://developers.cloudflare.com/cloudflare-one/identity/one-time-pin/).
- deploy (or re-deploy) the Tenzir stack including the Cloudflare proxy with
  `./tenzir-cloud deploy`
- start the Cloudflare tunnel with `./tenzir-cloud cloudflare.start`
- configure the tunnel routes to the deployed apps by running `./tenzir-cloud
  cloudflare.setup`
- in the Cloudflare Zero Trust pannel,
  [configure](https://developers.cloudflare.com/cloudflare-one/applications/configure-apps/self-hosted-apps/)
  the applications to protect the exposed endpoints
