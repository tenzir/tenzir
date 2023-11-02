---
title: Tenzir v4.4
authors: [dominiklohmann]
date: 2023-11-02
tags: [release, operators, velociraptor, yara]
---

We are incredibly excited to announce the release of [Tenzir
v4.4](https://github.com/tenzir/tenzir/releases/tag/v4.4.0). In keeping with our
commitment to advance with the rapidly-evolving field of security data
pipelines, we've focused this release on integrations with two pillars of the
security ecosystem: YARA and Velociraptor.

![Tenzir v4.4](tenzir-v4.4.excalidraw.svg)

<!-- truncate -->

## YARA Operator

The star feature of this release is the new YARA operator. You can now match
YARA rules directly within byte pipelines. This is a game-changer for threat
intelligence and cybersecurity workflows, as it brings together all of Tenzir's
connectors with the community's rich ecosystem of YARA rules for efficient
malware detection. Evaluating a set of rules on a file located in an S3 bucket
has never been easier:

```
load s3 bucket/file.ext
| yara path/to/rules/
```

:::info
We've written a blog post on the YARA operator that shows just how it works and
explains in-depth how you can use it: [Matching YARA Rules in Byte
Pipelines](blog/matching-yara-rules-in-byte-pipelines)
:::

## Velociraptor Operator

Velociraptor is an advanced digital forensic and incident response tool that
enhances your visibility into your endpoints. Not unlike Tenzir with TQL,
Velociraptor comes with its own language for interacting with it
programmatically: VQL. The `velocirator` operator makes it possible to query
Velociraptor directly from within Tenzir.

:::info
Read our blog post on how we built this integration and how you can utilize it
to learn more: [Integrating Velociraptor into Tenzir
Pipelines](blog/integrating-velociraptor-into-tenzir-pipelines)
:::

## Noteworthy Improvements

We provide a full list of changes [in our changelog](changelog#v440). Besides
the new operators, I would like to highlight the following changes to Tenzir:

- **Blob Type:** We've added a new `blob` type that allows you to handle binary
  data. Use the `blob` type over the `string` type for binary payloads that are
  not UTF8-encoded.

- **Rich Schema Inference for CSV:** Inferring schemas for CSV files has been
  significantly enhanced. It now provides more precise types, leading to more
  insightful analysis.

- **Automated Pipeline Management:** New controls for auto-restart, auto-delete
  and a runtime limit are now available when creating a pipeline. For a more
  granular control of the auto-restart and auto-delete behavior, the _Stopped_
  state for pipeline has now been divided into _Stopped_, _Completed_, and
  _Failed_. The states reflect whether a pipeline was manually stopped, ended
  naturally, or encountered an error, respectively.

- **Label Support for Pipelines:** Now, you can visually group related pipelines
  using the new labels feature. This helps you in organizing your pipelines
  better for improved visibility and accessibility.

Check out the new features on [app.tenzir.com](https://app.tenzir.com). We're
excited to see the amazing things you will accomplish with them!

Your feedback matters and drives our growth. Join the discussion in [our
Discord](/discord)!
