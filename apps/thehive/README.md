# VAST - TheHive integration

`VAST` is perfect companion for Security specialists, threat hunters, SOC analysts who are utilizing TheHive. Using Vast via multiple interfaces (analyzer, webhook, etc), investigations can be easily enriched and contextualized.

Example `docker-compose.yml` can be customized to fit any requirements. After instantiating environment, manual integration between TheHive and Cortex should be done.

## Vast Cortex Analyzer
We provide standalone analyzer and dockerized version under [Cortex Analyzer](analyzers/Vast/). For local development and debugging make sure to include `docker.autoUpdate=false` in [Cortex Config](vol/cortex/application.conf) and build/pull the image into local image repository.

Currently `Vast Cortex Analyzer` Docker image is not provided under official [Cortex Neurons Library](https://hub.docker.com/u/cortexneurons). Due to this, provided [analyzers.json](analyzers/local-analyzers.json) should be included in [Cortex Config](vol/cortex/application.conf):

```
## ANALYZERS
analyzer {
  urls = [
    #"https://download.thehive-project.org/analyzers.json",
    "/opt/cortex/analyzers/local-analyzers.json"
    #"/opt/cortex/analyzers"
  ]
}
```

For official documentation and clarifications on `Cortex` config, you can visit [official config](https://github.com/TheHive-Project/Cortex/blob/master/conf/application.sample)
