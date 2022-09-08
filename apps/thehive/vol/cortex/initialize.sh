#!/bin/bash
set -e

HOST="http://cortex:9001"

install_prerequisites() {
    apt-get update && apt-get install -y curl
}

wait_until_alive() {
    url="$HOST/index.html"
    echo "Trying until Cortex is alive.."
    until $(curl --output /dev/null --silent --head --fail $url); do
        printf 'Waiting...'
        sleep 5
    done
    echo "Cortex is alive."
}


http_request() {
    endpoint=$1
    payload=$2
    expected_status_code=$3
    is_authenticated="${4:-false}"
    credentials="${5:-admin@thehive.local:secret}"

    if $is_authenticated ; then
        response=$(curl -u $credentials --silent -o /dev/null -w "%{response_code}" -XPOST -H 'Content-Type: application/json' "$HOST$endpoint" -d "$payload")
    else
        response=$(curl --silent -o /dev/null -w "%{response_code}" -XPOST -H 'Content-Type: application/json' "$HOST$endpoint" -d "$payload")
    fi
    
    if [ $response != $expected_status_code ] ; then
        { echo "Error on $HOST$endpoint with ResponseCode: $response \n"; exit $ERRCODE; }
    fi
}

initialize_cortex() {
    http_request "/api/maintenance/migrate" "{}" 204 false
    http_request "/api/user" '{"login":"admin@thehive.local","name":"admin","password":"secret","roles":["superadmin"],"organization":"cortex"}' 201 false
    http_request "/api/organization" '{"name": "Tenzir", "description": "tenzir", "status": "Active"}' 201 true
    http_request "/api/user" '{"name": "Tenzir org Admin", "roles": ["read","analyze","orgadmin"], "organization": "Tenzir", "login": "orgadmin@thehive.local"}' 201 true
    http_request "/api/user/orgadmin@thehive.local/password/set" '{"password":"secret"}' 204 true
    http_request "/api/organization/analyzer/VAST-Search_1_0" '{"name":"VAST-Search_1_0","configuration":{"endpoint":"localhost:42000","max_events":40,"auto_extract_artifacts":false,"check_tlp":false,"max_tlp":2,"check_pap":false,"max_pap":2},"jobCache":10,"jobTimeout":30}' 201 true "orgadmin@thehive.local:secret"
}


install_prerequisites

wait_until_alive

initialize_cortex

echo "Accounts: [admin@thehive.local:secret, orgadmin@thehive.local:secret]"
echo "Done."



