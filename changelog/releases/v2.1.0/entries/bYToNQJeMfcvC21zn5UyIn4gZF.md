---
title: "Base image with Closed Source plugins"
type: feature
author: rdettai
created: 2022-07-18T15:10:29Z
pr: 2415
---

The VAST Cloud CLI can now authenticate to the Tenzir private registry and
download the vast-pro image (including plugins such as Matcher). The deployment
script can now be configured to use a specific image and can thus be set to use
vast-pro.
