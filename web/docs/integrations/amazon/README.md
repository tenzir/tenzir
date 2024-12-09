---
sidebar_position: 1
sidebar_label: Amazon Web Services
---

# Amazon Web Services

Tenzir integrates with the services from [Amazon Web
Services](https://aws.amazon.com) listed below.

## Configuration

To interact with AWS services, you need to provide appropriate credentials. This
defaults to using AWS's [default credentials provider
chain](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html).

Make sure to configure AWS credentials for the same user account that runs
`tenzir` and `tenzir-node`. The AWS CLI creates configuration files for the
current user under `~/.aws`, which can only be read by the same user account.

The `tenzir-node` systemd unit by default creates a `tenzir` user and runs as
that user, meaning that the AWS credentials must also be configured for that
user. The directory `~/.aws` must be readable for the `tenzir` user.

If a config file `<prefix>/etc/tenzir/plugin/$PLUGIN.yaml` or
`~/.config/tenzir/plugin/$PLUGIN.yaml` exists, it is always preferred over the
default AWS credentials. Here, `$PLUGIN` is the Tenzir plugin name, such as `s3`
or `sqs`. The configuration file must have the following format:

```yaml
access-key: your-access-key
secret-key: your-secret-key
session-token: your-session-token (optional)
```

import DocCardList from '@theme/DocCardList';

<DocCardList />
