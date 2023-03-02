---
sidebar_position: 1
---

# Cloud matchers

import CommercialPlugin from '@site/presets/CommercialPlugin.md';

<CommercialPlugin />

We provide a reference architecture and deployment scripts to have matchers
running in the AWS cloud.

In order to deploy VAST in the AWS cloud with the Pro image, follow the steps
described in the [deployment guide](../../setup/deploy/aws-pro.md).

## Architecture

To deploy matchers in the cloud, we need to instantiate two main cloud
resources:
- an SQS queue to reliable store and distribute the matched events
- a long running Fargate client that will attach to the VAST server and publish
  the matches to the queue

![AWS
Architecture](https://user-images.githubusercontent.com/7913347/184834597-cc6ef751-2444-4741-aacf-f9f7fdb9482d.png)

:::warning Intended to be replaced by the Fabric
Using SQS here is just a POC. It will be replaced in the near future by the
Fabric.
:::

## Setup

You first need to setup the base configuration as described in the [deployment
guide](../../setup/deploy/aws-pro.md). The only difference is that you
should activate the matcher plugin in the `.env` file:
```
VAST_CLOUD_PLUGIN = pro,matcher
```

After updating the config, run:
```bash
./vast-cloud deploy
```

You can then create [VAST
matchers](../../use/detect/match-threat-intel.md#start-matchers) through the
Lambda client:

```bash
./vast-cloud vast.lambda-client -c "vast matcher start --mode=exact --match-types=ip feodo"
```

Similarly, you can load indicators into the created matchers.

We provide scripts that create and load matchers from external feeds such as the
[Feodo Tracker](https://feodotracker.abuse.ch/):

```bash
./vast-cloud vast.lambda-client -c file://$(pwd)/resources/scripts/matcher/feodo.sh
```

Note: `vast.lambda-client` requires an absolute path when running a script from file.

Once the matchers are created, start the matcher client that will publish all
matches to the managed queue:

```bash
./vast-cloud matcher.start-client
```

## Example usage

:::warning Dependencies
To run this example, you need to enable the `workbucket` and `tests` cloud
plugins then run:

```
./vast-cloud deploy
```
:::

The matcher will trigger when events containing the registered IoC are imported
to VAST. We provide a flowlogs extract containing traffic that is currently
flagged by the Feodo tracker in the test datasets:

```
./vast-cloud tests.import-data --dataset=flowlogs
```

You can listen to matched events published on AWS SQS:

```
./vast-cloud matcher.attach
```

:::tip
Matched events are kept in the queue only for a few minutes.
:::
