# Write a Package

This tutorial walks you through the creation of a [package](packages.md), which
is a bundle of related pipelines and contexts. You can [install
packages](installation/install-a-package.md) with a few clicks from the [Tenzir
Library](https://app.tenzir.com/library) or deploy them as code.

## Map the use case

The goal of a package is to enable a specific use case. In this tutorial, we
want to make it easy to detect malicious certificates that are on the
[SSLBL](https://sslbl.abuse.ch/) from [abuse.ch](https://abuse.ch). At a glance,
the idea is as follows:

1. We get SHA1 hashes of SSL certifcates from network monitor logs, such as
   [Zeek](https://zeek.org) or [Suricata](https://suricata.io)
2. We check each hash value against a lookup table.
3. We generate a detection finding when we encounter a match.

Given this idea, we have to it to the building blocks we have in Tenzir:
[pipelines](pipelines.md) and [contexts](contexts.md). We can model it as
follows.

1. A lookup table that includes a copy of the SSLBL data.
2. A pipeline that synchronizes the SSLBL data with the lookup table.
3. A pipeline to enrich network telemetry and generate detection findings.

## Create the scaffold

We begin with creating a `package.yaml` file with the following metadata:

```yaml title="package.yaml"
id: sslbl
name: SSLBL
author: Tenzir
author_icon: https://github.com/tenzir.png
package_icon: |
  https://raw.githubusercontent.com/tenzir/library/main/sslbl/package.svg
description: |
  The [SSLBL](https://sslbl.abuse.ch/) package makes available a lookup table
  with SHA1 hashes of blacklisted certificates that can be used when monitoring
  SSL/TLS certificate exchanges.
```

## Add your pipelines, context, and examples

After providing the package metadata, we now do the heavy lift of writing pipelines, contexts, and examples.

### Add a context

First, we need a data structure to hold the SSLBL data so that we can use it
inside the node. TO this end, we define a lookup table in the `contexts`
section:

```yaml title="package.yaml"
contexts:
  feodo:
    type: lookup-table
    description: |
      A table that is keyed by SHA1 hashes of SSL certificates on the SSL
      blocklist.
```

Let's figure out how to get data into the context manually. There's a CSV file
at <https://sslbl.abuse.ch/blacklist/sslblacklist.csv>. Let's take a look at
that in the browser:

```csv
################################################################
# abuse.ch SSLBL SSL Certificate Blacklist (SHA1 Fingerprints) #
# Last updated: 2024-09-02 06:17:52 UTC                        #
#                                                              #
# Terms Of Use: https://sslbl.abuse.ch/blacklist/              #
# For questions please contact sslbl [at] abuse.ch             #
################################################################
#
# Listingdate,SHA1,Listingreason
2024-09-02 06:17:52,4da14224452c1fe61f46b1112c43ecfd9f322c82,Rhadamanthys C&C
2024-09-02 06:16:33,b331526a0949f88ce218555edf1060c4a02de5a2,Rhadamanthys C&C
2024-09-02 05:19:10,b785d1a9e5784703b98a96698ad05dff5e07229a,DCRat C&C
2024-09-02 05:18:33,be4d4f077bdd90618367eb2ab88f9f3074ccb7aa,AsyncRAT C&C
2024-08-30 13:46:12,6bef207908bfad6b19580067ce770bc820d3d2ef,CobaltStrike C&C
2024-08-29 09:02:20,9a57379949f734b11f32bb8462db1f3fd8898722,DarkGate C&C
2024-08-29 09:01:39,47122f7861b7488f6711c999e40caaa5e560630b,Rhadamanthys C&C
2024-08-29 09:01:33,fb0e13607d045047c29ab44941f450950a349dd7,Rhadamanthys C&C
```

Okay, a simple CSV table with comments. We can write a pipeline for reading
that:

```tql
// tql2
load_http "from https://sslbl.abuse.ch/blacklist/sslblacklist.csv"
read_csv comments=true, header="timestamp,SHA1,reason"
```

Now that we have onboarded the data into a pipeline, we just need to push it
into the context by piping it to `context update`:

```tql
// tql2
load_http "from https://sslbl.abuse.ch/blacklist/sslblacklist.csv"
read_csv comments=true, header="timestamp,SHA1,reason"
legacy "context update sslbl --key=SHA1"
```

With `context inspect sslbl` we can list the table contents, keyed by SHA1 hash
and ready for enrichment.

### Keep the context synchronized

So far we did a one-shot download of the SSLB data into our lookup table. But
the fine folks at abuse.ch update the data regularly, and we want to keep our
lookup table in sync with the latest version.

To this end, we execute that pipeline periodically using `every`:

```tql
// tql2
every 1h {
  load_http "from https://sslbl.abuse.ch/blacklist/sslblacklist.csv"
  read_csv comments=true, header="timestamp,SHA1,reason"
  legacy "context update sslbl --key=SHA1"
}
```

Now let's copy that into the package definition:

```yaml title="package.yaml"
pipelines:
  update-context:
    name: Update SSLBL Context
    description: |
      A pipeline that periodically refreshes the SSLBL lookup table.
    definition: |
      // tql2
      every 1h {
        load_http "from https://sslbl.abuse.ch/blacklist/sslblacklist.csv"
        read_csv comments=true, header="timestamp,SHA1,reason"
        legacy "context update sslbl --key=SHA1"
      }
    restart-on-error: 1 hour
```

From a developer's perspective, we now have a complete package consisting of a
context and a pipeline that updates it. But from a user's perspective, what do
we do now? This is where the `examples` section comes into play.

### Entice with examples

After you put thought into implementing the intricate dataflows and thoughtfully
configured pipelines and context, it's time to switch from the developer to the
user persona. As a package developer, you want to make things reusable and easy!
In the `examples` section you showcase how users can profit from the work that
the package does behind the scenes.

For our concrete scenario, we now want to use the SSLBL context that we set up
and keep up to date. Our "work" was allowing users to just come with a SHA1 hash
digest, and the context quickly tells us good or bad.

```yaml title="package.yaml"
examples:
  - name: Enrich Suricata TLS logs with SSLBL domains
    description: |
      Enriches the certificate SHA1 fingerprint from Suricata TLS logs with the
      SSLBL data.
    definition: |
      // tql2
      subscribe "suricata"
      | where @name == "suricata.tls"
      | set sha1 = tls.fingerprint.replace(":", "")
      | legacy "enrich sha1 sslbl"

  - name: Display top-10 listing reasons
    description: |
      Shows a bar chart of the top-10 reasons why a certificate is in the
      dataset.
    definition: |
      context inspect sslbl
      | yield value
      | top reason
      | head
      | chart bar
```

## Make the package configurable

After we have illustrated how the package works with examples, let's step back
for a moment and assess how customizable the package should be:

- Is there anything that might differ from user to user?
- Do they have to bring their API key for the package to work?
- Are timeouts highly subjective and specific to the local environment?

For all places where it's difficult to offer a one-size-fits-all assumption, we
want the user to make decision on how to proceed. These customization points are
called *inputs*, and the correspondingly named section in the package definition
specifies them.

In our case, we hard-coded the refresh interval that updates the SSLBL lookup
table to exactly 1 hour. Maybe other users want to update just once a day? Or
more often? This is a typical example where policy is user-specific and where we
can turn an assumption into a decision. Let's define the input:

```yaml title="package.yaml"
inputs:
  refresh-interval:
    name: Time between context updates
    description: |
      The duration between updates that fetch the SSLBL database via the API.
    default: 1 hour
```

By specifying a default value, we also make it easy for users to skip the
decision making. In other words, defaults make a configuration knob optional.

After we've defined our configuration knobs, we now go over the pipeline
definitions and replace the hard-coded constants with a placeholder:

```yaml title="package.yaml"
pipelines:
  update-context:
    name: Update SSLBL Context
    description: |
      A pipeline that periodically refreshes the SSLBL lookup table.
    definition: |
      // tql2
      every {{ inputs.refresh-interval }} {
        load_http "from https://sslbl.abuse.ch/blacklist/sslblacklist.csv"
        read_csv comments=true, header="timestamp,SHA1,reason"
        legacy "context update sslbl --key=SHA1"
      }
    restart-on-error: 1 hour
```

Note how we simply replaced `1h` with `{{ inputs.refresh-interval }}`.

## Test your package

When you think you're done, it's time to validate that things work as you
expect. This means effectively trying to install, configure and use it.

:::note Testing Framework
We currently don't offer a native testing framework for packages where you can
provide tests and baselines along the package definition. But we love the idea,
and if you do as well, please swing by our [Community Discord](/discord) and
discuss it with us.
:::

[Installing a package](installation/install-a-package.md) given a `package.yaml`
file is easiest with the [`package_add`](operators/package.md) operator, since
a package is just data:

```tql
load_file "/path/to/package.yaml"
read_yaml
package_add
```

This fails with the following error:

```
error: failed to add package
 = note: with error: !! unspecified: named argument `header` does not exist
 = note: failed to add package
```

Doh, we didn't substitute the template `{{ inputs.refresh-interval }}`. We can
do this with one extra statement, though:

```tql
load_file "/path/to/package.yaml"
read_yaml
set config.inputs["refresh-interval"] = 1h
package_add
```

NB: We have to access the field `refresh-interval` in `config.inputs` via `[]`
because `config.inputs.refresh-interval` would be parsed as a subtraction
between two fields.

The package should show up in the list of packages after installation:

```
// tql2
packages
where id == "sslbl"
```

The pipelines that came with the package also have its ID prefixed:

```
// tql2
pipelines
where id.starts_with("sslbl")
```

And the context is also there:

```
// tql2
contexts
where id.starts_with("sslbl")
```

Since we checked that everything works as expected, we now remove our package
with:

```tql
// tql2
package_remove "sslbl"
```

## Share and contribute

🙌 Fantastic, you've just wrapped a use case and made it accessible to a broader
audience! Now spread the word and share it with the broader community of Tenzir
users for an even bigger impact. Here's how:

1. Join our [Discord server](/discord) and showcase your package in the
   show-and-tell channel. We encourage you to seek feedback to make your package
   even better.
2. File a pull request in the official [Community
   Library](https://github.com/tenzir/library) GitHub repository. All packages
   in there will automatically show up in the [Tenzir
   Library](https://app.tenzir.com/library).
3. Share it on social media and let us know. We'll amplify it! 🫶
