# VAST Neuron

VAST is an embeddable telemetry engine for structured event data, purpose-built
for use cases in security operations. VAST is an acronym and stands for
Visibility Across Space and Time.

The analyzer takes following input datatype:
1. ip
2. subnet (custom)
3. domain
4. hash

## Requirements for the Non-Dockerized Analyzer
- VAST binary should be available on Cortex host. We are providing example
  [Cortex Dockerfile](/thehive/cortex/Dockerfile). Please refer to [VAST
  Documentation](vast.io) for instructions on manual installation.
- Install [analyzer dependencies](/thehive/analyzers/VAST/requirements.txt) on
  the host by `pip3 install -r requirements.txt`
- VAST server address should be provided as a parameter in Cortex

## Requirements for the Dockerized Analyzer
- Currently `VAST Cortex Analyzer` Docker image is not provided under official
  [Cortex Neurons Library](https://hub.docker.com/u/cortexneurons)
- Build and push analyzer image to local image repo. We are providing
  [analyzers.json](analyzers/local-analyzers.json) to be used in [Cortex
  Config](cortex/application.conf):

    ```
    ## ANALYZERS
    analyzer {
      urls = [
        "/opt/cortex/analyzers/local-analyzers.json"
      ]
    }
    ```
- For local development and debugging make sure to include
  `docker.autoUpdate=false` in [Cortex Config](cortex/application.conf) and
  build/pull the image into local image repository.

## How to test and debug

To run this Neuron individually, in the `tests` directory you will find:
- a `run` script with some scafolding around the Docker commands
  - use `tests/run host` if VAST is running on your localhost
  - use `tests/run service` if VAST is running as a Compose service
- an `input` directory with example input files (mounted as `input.json`)
- an `output` directory where the resulting output is written
