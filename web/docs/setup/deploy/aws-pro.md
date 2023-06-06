---
sidebar_position: 3
---

# AWS with Pro version

import CommercialPlugin from '@site/presets/CommercialPlugin.md';

<CommercialPlugin />

Before running Tenzir Pro on AWS, you should refer to the [deployment
guide](aws.md) of the base stack.

To enable the use of Commercial features such as matchers you need to use the
Tenzir Pro image:
- Set up the version you plan to use and the activate the `pro` plugins in
  the `.env` file:
```
TENZIR_CLOUD_PLUGINS=pro
TENZIR_VERSION=latest
```
- Authenticate and download the Pro image from Tenzir's private repository:
```bash
./tenzir-cloud pro.login pro.pull-image
```
- Configure the deployment as explained in the [deployment
  guide](aws.md):
```
TENZIR_PEERED_VPC_ID=vpc-059a7ec8aac174fc9
TENZIR_CIDR=172.31.48.0/24
TENZIR_AWS_REGION=eu-north-1
TENZIR_IMAGE=tenzir/tenzir-pro
```
- (Re)Deploy the Tenzir server:
```bash
./tenzir-cloud deploy -a
```
- Verify that you are running Tenzir Pro (commercial features such as `matcher`
  should appear in the `plugins` list of the response)
```bash
./tenzir-cloud run-lambda -c "tenzir version"
```
- Start (or restart) the Tenzir server:
```bash
./tenzir-cloud [re]start-tenzir-server
```

You can now use commercial features such as
[matchers](../../use/detect/cloud-matchers.md) in the Cloud!
