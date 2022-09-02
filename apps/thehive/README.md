# VAST - TheHive integration

`VAST` is perfect companion for Security specialists, threat hunters, SOC analysts who are utilizing TheHive. Using VAST via multiple interfaces (analyzer, webhook, etc), investigations can be easily enriched and contextualized.

Use `docker-compose` with provided `docker-compose.yml` to create a new environment and test the integrations. After instantiating environment, make sure to manually integrate Cortex/TheHive by creating users and generating API Keys.

```
docker-compose up -d
```

## VAST Cortex Analyzer
We provide standalone analyzer and dockerized version under [Cortex Analyzer](analyzers/VAST/).

## Notes
For official documentation on `Cortex` config, you can visit [official config](https://github.com/TheHive-Project/Cortex/blob/master/conf/application.sample)
