---
title: "PRs 1045-1055-1059-1062"
type: change
author: mavam
created: 2020-09-10T12:12:23Z
pr: 1045
---

The proprietary VAST configuration file has changed to the more ops-friendly
industry standard YAML. This change introduced also a new dependency:
[yaml-cpp](https://github.com/jbeder/yaml-cpp) version 0.6.2 or greater. The
top-level `vast.yaml.example` illustrates how the new YAML config looks like.
Please rename existing configuration files from `vast.conf` to `vast.yaml`.
VAST still reads `vast.conf` but will soon only look for `vast.yaml` or
`vast.yml` files in available configuration file paths.
