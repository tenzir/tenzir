# VAST Neuron

VAST is an embeddable telemetry engine for structured event data, purpose-built for use cases in security operations. VAST is an acronym and stands for Visibility Across Space and Time.

The analyzer takes following input datatype:
1. ip
2. subnet (custom)
3. domain
4. hash

## Requirements For Non-Dockerized Analyzer
- VAST binary should be available on Cortex host. We are providing example [Cortex Dockerfile](/thehive/vol/cortex/Dockerfile). Please refer to [VAST Documentation](vast.io) for instructions on manual installation.
- Install [analyzer dependencies](/thehive/analyzers/VAST/requirements.txt) on the host by `pip3 install -r requirements.txt`
- VAST server address should be provided as a parameter in Cortex

## Requirements For Dockerized Analyzer
- Currently `VAST Cortex Analyzer` Docker image is not provided under official [Cortex Neurons Library](https://hub.docker.com/u/cortexneurons)
- Build and push analyzer image to local image repo. We are providing [analyzers.json](analyzers/local-analyzers.json) to be used in [Cortex Config](vol/cortex/application.conf):

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
- For local development and debugging make sure to include `docker.autoUpdate=false` in [Cortex Config](vol/cortex/application.conf) and build/pull the image into local image repository.
- If you are utilizing `VAST Server` as containerized on host, make sure to include `docker.container.networkMode=host` in [Cortex Config](vol/cortex/application.conf) and use local addres to reach. Example: `localhost:42000`

## How to Debug
- Create `/input` and `/output` folders in where the analyzer script runs
- Put a file named `input.json` into input folder with example input:
    ```
    {
        "data": "10.12.14.101",
        "dataType": "ip",
        "tlp": 2,
        "pap": 2,
        "message": "",
        "parameters": {},
        "config": {
        "proxy_https": null,
        "cacerts": null,
        "check_tlp": false,
        "max_tlp": 2,
        "auto_extract_artifacts": false,
        "max_events": 30,
        "jobCache": 10,
        "check_pap": false,
        "max_pap": 2,
        "endpoint": "127.0.0.1:42000",
        "jobTimeout": 30,
        "service": "get",
        "proxy_http": null
    }
    ```
- While running the script, provide `.` as second parameter to indicate `job-directory` is current folder

## Example launch.json file for VSCode
```
    {
    "version": "0.2.0",
    "configurations": [
            {
                "name": "Python: Main file",
                "type": "python",
                "request": "launch",
                "program": "search.py",
                "console": "integratedTerminal",
                "justMyCode": false,
                "args": ["."]
            },
            {
                "name": "Python: Current File",
                "type": "python",
                "request": "launch",
                "program": "${file}",
                "console": "integratedTerminal",
                "justMyCode": false
            }
        ]
    }
````