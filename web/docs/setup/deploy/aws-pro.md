---
sidebar_position: 3
---

# AWS with Pro version

import CommercialPlugin from '@site/presets/CommercialPlugin.md';

<CommercialPlugin />

Before running VAST Pro on AWS, you should refer to the [deployment
guide](aws.md) of the base stack.

To enable the use of Commercial features such as matchers you need to use the
VAST Pro image:
- Set up the version you plan to use and the activate the `pro` plugins in
  the `.env` file:
```
VAST_CLOUD_PLUGINS=pro
VAST_VERSION=latest
```
- Authenticate and download the Pro image from Tenzir's private repository:
```bash
./vast-cloud pro.login pro.pull-image
```
- Configure the deployment as explained in the [deployment
  guide](aws.md):
```
VAST_PEERED_VPC_ID=vpc-059a7ec8aac174fc9
VAST_CIDR=172.31.48.0/24
VAST_AWS_REGION=eu-north-1
VAST_IMAGE=tenzir/vast-pro
```
- (Re)Deploy the vast server:
```bash
./vast-cloud deploy -a
```
- Verify that you are running VAST Pro (commercial features such as `matcher`
  should appear in the `plugins` list of the response)
```bash
./vast-cloud run-lambda -c "vast version"
```
- Start (or restart) the VAST server:
```bash
./vast-cloud [re]start-vast-server
```

You can now use commercial features such as
[matchers](../../use/detect/cloud-matchers.md) in the Cloud!
