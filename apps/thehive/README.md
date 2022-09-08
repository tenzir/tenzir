# VAST - TheHive integration

`VAST` is perfect companion for Security specialists, threat hunters, SOC analysts who are utilizing TheHive. Users of TheHive can enrich and contextualize their investigations effortlessly with `VAST` integrations.

Use `docker-compose` with provided `docker-compose.yml` to create a new environment and test the integrations. After instantiating environment, service `cortex-initializer` sets-up Cortex with VAST analyzer with users: `admin@thehive.local:secret` and `orgadmin@thehive.local:secret`. 

```
docker-compose up -d
```

## VAST Cortex Analyzer
We provide standalone analyzer and dockerized version under [Cortex Analyzer](analyzers/VAST/).

## Notes
For official documentation on `Cortex` config, you can visit [official config](https://github.com/TheHive-Project/Cortex/blob/master/conf/application.sample)
