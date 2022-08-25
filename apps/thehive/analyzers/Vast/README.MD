# Vast Neuron

VAST is an embeddable telemetry engine for structured event data, purpose-built for use cases in security operations. VAST is an acronym and stands for Visibility Across Space and Time.

The analyzer takes following input datatype:
1. ip
2. subnet (custom)
3. domain
4. hash

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
- While running the script in debug mode, provide `.` as second parameter to indicate `job-directory` is current folder
- Example `launch.json` file for VSCode
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
## Requirements
- Vast binary should be available on host. Please refer to [Vast Documentation](vast.io) for instructions
- Dependencies in reuirements.txt should be installed
- Vast server address should be provided as a parameter in Cortex