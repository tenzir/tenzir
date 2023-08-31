# Setup

This section describes Tenzir from an **operator perspective**. We cover the
different stages of the setup process to successfully run Tenzir. You have
several options to enter the setup process, based on what intermediate step
you would like to begin with.

```mermaid
flowchart LR
  classDef action fill:#00a4f1,stroke:none,color:#eee
  classDef artifact fill:#bdcfdb,stroke:none,color:#222
  %% Actions
  download(Download):::action
  build(Build):::action
  install(Install):::action
  deploy(Deploy):::action
  configure(Configure):::action
  tune(Tune):::action
  monitor(Monitor):::action
  %% Artifacts
  source([Source Code]):::artifact
  binary([Binary]):::artifact
  deployable([Package/Image]):::artifact
  instance([Instance]):::artifact
  %% Edges
  download --> source
  download --> binary
  download --> deployable
  source --> build
  build --> binary
  binary --> install
  install --> deployable
  deployable --> deploy
  deploy --> instance
  instance <--> configure
  instance <--> tune
  instance <--> monitor
  %% Links
  click download "setup/download" "Download Tenzir"
  click build "setup/build" "Build Tenzir"
  click install "setup/install" "Install Tenzir"
  click deploy "setup/deploy" "Deploy Tenzir"
  click configure "setup/configure" "Configure Tenzir"
  click tune "setup/tune" "Tune Tenzir"
  click monitor "setup/monitor" "Monitor Tenzir"
```
