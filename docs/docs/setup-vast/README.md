# Setup VAST

This section covers the different stages of the setup process that ultimately
yield a running VAST instance. You have several options to enter the setup
pipeline, based on what intermediate artifact you would like to begin with.

👇 Click on any blue actions to get started.

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
  %% Links
  click download "/vast/docs/setup-vast/download/" "Download VAST"
  click build "/vast/docs/setup-vast/build/" "Build VAST"
  click install "/vast/docs/setup-vast/install/" "Install VAST"
  click deploy "/vast/docs/setup-vast/deploy/" "Deploy VAST"
  click configure "/vast/docs/setup-vast/configure/" "Configure VAST"
  click tune "/vast/docs/setup-vast/tune/" "Tune VAST"
```
