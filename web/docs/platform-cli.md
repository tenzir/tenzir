# Platform CLI

The *Tenzir Platform CLI* allows users to interact with the *Tenzir Platform* from the command-line
to manage their workspaces and nodes.

## Installation

Install the [`tenzir-platform`](https://pypi.org/project/tenzir-platform/)
package from PyPI.

```bash
pip install tenzir-platform
```

## Authentication

### Synopsis

```
tenzir-platform auth login
tenzir-platform workspace list
tenzir-platform workspace select <workspace_id>
```

### Description

The `tenzir-platform auth login` command authenticates the current user.

The `tenzir-platform workspace list` and `tenzir-platform workspace select`
commands show workspaces available to the authenticated user and select one,
respectively.

#### `<workspace_id>`

The unique ID of the workspace, as shown in `tenzir-platform workspace list`.

## Manage Nodes

### Synopsis

```
tenzir-platform node list
tenzir-platform node ping <node_id>
tenzir-platform node create [--name <node_name>]
tenzir-platform node delete <node_id>
tenzir-platform node run [--name <node_name>] [--image <container_image>]
```

### Description

The following commands interact with the selected workspace. See [Authentication](#authentication)
above for how to select a workspace:
- `tenzir-platform node list` lists all nodes in the selected workspace,
  including their ID, name, and connection status.
- `tenzir-platform node ping` pings the specified node.
- `tenzir-platform node create` registers a new node at the platform so that it
  can be connected to the platform. Note that this neither starts a new node nor
  configures one, it just creates a new API key that a node can use to connect
  to the platform with.
- `tenzir-platform node delete` removes a node from the platform. Note that this
  does not stop the node, it just removes it from the platform.
- `tenzir-platform node run` creates and registers an ad-hoc node, and starts it
  on the local host. Requires Docker Compose to be available.
  The node is temporary and will be deleted when the `run` command is stopped.

#### `<node_id>`

The unique ID of the node, as shown in `tenzir-platform node list`.

#### `<node_name>`

The name of the node as shown in the app.

#### `<container_image>`

The Docker image to use for ad-hoc created node. We recommend using one of the
following images:
- `tenzir/tenzir:v4.11.2` to use the specified release.
- `tenzir/tenzir:latest` to use the last release.
- `tenzir/tenzir:main` to use the currnet development version.

## Manage Secrets

The Tenzir Platform provides storage of secrets, which can be accessed by pipelines running on a Tenzir Node.

### Synopsis

```
  tenzir-platform secret add <name> [--file=<file>] [--value=<value>] [--env]
  tenzir-platform secret update <secret_id> [--file=<file>] [--value=<value>] [--env]
  tenzir-platform secret delete <secret_id>
  tenzir-platform secret list [--json]
```

### Description

The following commands allow for managing secrets in the Tenzir Platform. Secrets can be securely stored and accessed by pipelines running on nodes.

- `tenzir-platform secret add` adds a new secret to the platform. The secret value can be provided directly via the `--value` option, read from a file using the `--file` option, or sourced from an environment variable using the `--env` flag. The secret is identified by the provided `<name>`.

- `tenzir-platform secret update` updates an existing secret identified by `<secret_id>`. Similar to adding a secret, the new value can be provided via `--value`, `--file`, or `--env`.

- `tenzir-platform secret delete` removes a secret from the platform. The secret to be deleted is identified by its `<secret_id>`.

- `tenzir-platform secret list` lists all secrets available in the platform. The `--json` flag can be used to output the list in JSON format for easier integration with other tools.

#### `<name>`

The unique name to identify the secret. This name is used when accessing the secret in pipelines.

#### `<secret_id>`

The unique identifier of the secret, as shown in the output of `tenzir-platform secret list`.

#### `--file=<file>`

Specifies a file containing the secret value. The file's contents will be securely stored as the secret value.

#### `--value=<value>`

Specifies the secret value directly as a command-line argument.

#### `--env`

Indicates that the secret value should be sourced from an environment variable. The environment variable must be set before running the command.

#### `--json`

Outputs the list of secrets in JSON format when used with the `tenzir-platform secret list` command.

## Manage External Secret Stores

Instead of using the built-in secret store of the Tenzir Platform, users can
configure their workspaces to use secrets from an external secret store.

At the moment, only AWS Secrets Manager is supported as an external store,
although we're planning to add more options in the near future.

By default, external secret stores are mounted read-only, ie. it will not be
possible to add or update secrets from the CLI or the web interface. Some
external secret store implementations may offer options to also allow write
access to the secrets.

### Synopsis

```sh
tenzir-platform secret store add aws
    --region=<region>
    --assumed-role-arn=<assumed_role_arn>
    [--name=<name>]
    [--prefix=<prefix>]
    [--access-key-id=<key_id>]
    [--secret-access-key=<key>]
tenzir-platform secret store set-default <store_id>
tenzir-platform secret store delete <store_id>
tenzir-platform secret store list [--json]
```

### Description

When the Tenzir Node accesses a secret by using the `secret("foo")` function
in a pipeline, the Tenzir Platform will look up the secret with the name `foo`
in the default secret store of that node's workspace and return the secret
value to the node.

When using an external secret store, the Tenzir Platform needs to have the
necessary permissions to read secret value from that store.

For the AWS Secrets Manager, the Tenzir Platform uses STS to assume a role with
the necessary permissions. It is your responsibility to create that role and

1) Create an IAM user. Download access keys and pass them via the `--access-key-id`
   and `--secret-access-key` arguments. This approach is the easiest to set up,
   but only acceptable for self-hosted testing or development instances of the
   Tenzir Platform because it stores long-term credentials in the Tenzir Platform.

2) Assign the permissions to the task role of the websocket gateway.
   This the recommended option for users running the Sovereign Edition of the
   Platform and deployed it to their own AWS account.

3) Assign the permission to assume the configured role to Tenzir's AWS account
   with the id `660178929208`. This option will only work with our publicly
   hosted platform instance on `https://app.tenzir.com`.

We're working on expanding this to offer an OIDC-based option as well, as an
alternative to options (1) and (3) above.

## Manage Alerts

### Synopsis

```
tenzir-platform alert add <node> <duration> <webhook_url> [<webhook_body>]
tenzir-platform alert delete <alert_id>
tenzir-platform alert list
```

### Description

The following commands allow for setting up alerts for specific nodes. After a
node has been disconnected for the configured amount of time, an alert fires by
performing a POST request against the configured webhook URL.

#### `<node>`

The node to be monitored. Can be given provided as a node ID or a node name,
as long as the name is unambiguous.

#### `<duration>`

The amount of time to wait between the node disconnect and triggering the alert.

#### `<webhook_url>`

The platform performs a POST request against this URL when the alert triggers.

#### `<webhook_body>`

The body to send along with the webhook. Must be valid JSON. The body may
contain the string `$NODE_NAME`, which will be replaced by the name of the
node that triggered the alert.

Defaults to `{"text": "Node $NODE_NAME disconnected for more than {duration}s"}`,
where `node_id` and `duration` are set dynamically from the CLI parameters.

### Example

Given nodes like this:
```
$ tenzir-platform node list
🟢 Node-1 (n-w2tjezz3)
🟢 Node-2 (n-kzw21299)
🔴 Node-3 (n-ie2tdgca)
```

We want to receive a Slack notification whenever Node-3 is offline for more than 3 minutes.
First we create a webhook as described in the [Slack docs](https://api.slack.com/messaging/webhooks).
Next, we configure the alert in the Tenzir Platform:

```
$ tenzir-platform alert add Node-3 3m "https://hooks.slack.com/services/XXXXX/YYYYY/ZZZZZ" '{"text": "Alert! Look after node $NODE_NAME"}'
```

Unless Node-3 reconnects, we should see a message appear after 3 minutes in the configured Slack channel.

## Manage Workspaces

:::warning On-Premise Setup Required
This functionality of the CLI can only be used in combination
with an on-premise platform deployment, which is available to users
of the [Sovereign Edition](https://tenzir.com/pricing).
:::

These CLI commands are only available to local platform administrators.
The `TENZIR_PLATFORM_OIDC_ADMIN_RULES` variable described
[here](installation/deploy-the-platform#identity-provider-idp) is used
to define who is an administrator in your platform deployment.

### Synopsis

```
tenzir-platform admin list-global-workspaces
tenzir-platform admin create-workspace <owner_namespace> <owner_id> [--name <workspace_name>]
tenzir-platform admin delete-workspace <workspace_id>
```

### Description

The `tenzir-platform workspace admin list-global-workspaces`, `tenzir-platform
admin create-workspace`, and `tenzir-platform admin delete-workspace` commands
list, create, or delete workspaces, respectively.

#### `<owner_namespace>`

Either `user` or `organization`, depending on whether the workspace is
associated with a user or an organization.

The selected namespace will determine the *default* access rules for the
workspace:

 - For a user workspace, a single access rule will be created that allows
   access to the user whose user id matches the given `owner_id`
 - For an organization workspace, no rules will be created by default and
   they have to be manually added using the `add-auth-rule` subcommand
   described below.

#### `<owner_id>`

The unique ID of the workspace owner:
- If `<owner_namespace>` is `user`, then this matches the user's `sub` claim in
  the OIDC token.
- If `<owner_namespace>` is `organization`, then this is an arbitrary string
  uniquely identifiying the organization the workspace belongs to.

#### `--name <workspace_name>`

The name of the workspace as shown in the app.

#### `<workspace_id>`

The unique ID of the workspace, as shown in `tenzir-platform workspace list` or
`tenzir-platform admin list-global-workspaces`.

## Configure Access Rules

:::warning On-Premise Setup Required
This functionality of the CLI can only be used in combination
with an on-premise platform deployment, which is available to users
of the [Sovereign Edition](https://tenzir.com/pricing).
:::

These CLI commands are only available to local platform administrators.
The `TENZIR_PLATFORM_OIDC_ADMIN_RULES` variable described
[here](installation/deploy-the-platform#identity-provider-idp) is used
to define who is an administrator in your platform deployment.

### Synopsis

```
tenzir-platform admin list-auth-rules <workspace_id>
tenzir-platform admin add-auth-rule [--dry-run]
    email-domain <workspace_id> <connection> <domain>
tenzir-platform admin add-auth-rule [--dry-run]
    organization-membership <workspace_id> <connection> <organization_claim> <organization>
tenzir-platform admin add-auth-rule [--dry-run]
    organization-role <workspace_id> <connection> <roles_claim> <role> <organization_claim> <organization>
tenzir-platform admin add-auth-rule [--dry-run]
    user <workspace_id> <user_id>
tenzir-platform admin add-auth-rule [--dry-run]
    allow-all <workspace_id>
tenzir-platform admin delete-auth-rule <workspace_id> <auth_rule_index>
```

### Description

Users with admin permissions can additionally use the `tenzir-platform admin
list-auth-rules`, `tenzir-platform admin add-auth-rule`, and `tenzir-platform
admin delete-auth-rule` commands to list, create, or delete authentication rules
for all users, respectively.

Authentication rules allow users to access the workspace with the provided
`<workspace_id>` if the user's `id_token` matches the configured rule. Users
have access to a workspace if any configured rule allows access. The following
rules exist:

- **Email Suffix Rule**: `tenzir-platform admin add-auth-rule email-domain`
  allows access if the `id_token` contains a field `connection` that exactly
  matches the provided `<connection>` and a field `email` that ends with the
  configured `<domain>`.

- **Organization Membership**: `tenzir-platform admin add-auth-rule
  organization-membership` allows
  access if the `id_token` contains a field `connection` that exactly matches the
  provided `<connection>` and a field `<organization_claim>` that exactly matches
  the provided `<organization>`.

  Note that the `<organization_claim>` and `<organization>` can be freely
  chosen, so this rule can also be repurposed for generic claims that are not
  necessarily related to organizations.

- **Organization Role Rule**: `tenzir-platform admin add-auth-rule
  organization-role` allows access if the `id_token` contains a field
  `connection` that exactly matches the provided `<connection>`, a field
  `<organization_claim>` that exactly matches the provided `<organization>`, and
  a field `<roles_claim>` that must be a list containing a value exactly
  matching `<role>`.

  We recommend using organization role rules to check if a user has a specific
  role with an organization.

- **User Rule**: `tenzir-platform admin add-auth-rule user` allows access if the
  `id_token` contains a field `sub` that exactly matches the provided
  `<user_id>`.

- **Allow All Rule**: `tenzir-platform admin add-auth-rule allow-all` allows access
  to every user. This can be useful to e.g. set up a workspace that is shared by all
  users of a platform instance.
