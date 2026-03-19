---
title: Correct AWS Marketplace container image
type: change
author: lava
pr: 5925
created: 2026-03-19T10:11:21.195262Z
---

The AWS Marketplace ECR repository `tenzir-node` was incorrectly populated with
the `tenzir` image. It now correctly ships `tenzir-node`, which runs a Tenzir
node by default.

If you relied on the previous behavior, you can restore it by setting `tenzir`
as a custom entrypoint in your ECS task definition.
