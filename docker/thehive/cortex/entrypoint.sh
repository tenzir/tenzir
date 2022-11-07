#!/bin/bash

# Fill the missing variables in the configuration file and run the original
# entrypoint

set -e

if [[ ! -v ELASTICSEARCH_URI || ! -v CORTEX_NETWORK_MODE  ]]; then
    echo "ELASTICSEARCH_URI and CORTEX_NETWORK_MODE need to be set"
    exit 1
fi

echo $ELASTICSEARCH_URI
echo $CORTEX_NETWORK_MODE

sed -i "s@__ELASTICSEARCH_URI_PLACEHOLDER__@$ELASTICSEARCH_URI@" /etc/cortex/application.conf
sed -i "s@__CORTEX_NETWORK_MODE_PLACEHOLDER__@$CORTEX_NETWORK_MODE@" /etc/cortex/application.conf

exec /opt/cortex/entrypoint
