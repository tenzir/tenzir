#!/bin/bash

# Activate event bridge notifications on specified AWS S3 bucket, without
# disturbing existing notification configurations. The first argument is the
# bucket region, the second one its name. This is meant to be run by Terragrunt
# in a hook.

echo "Running script: ./$(basename $0) $1 $2"

current_config=$(aws s3api --region $1 get-bucket-notification-configuration --bucket $2)

if [ $? -ne 0 ]
then 
  echo "- Failed to fetch bucket notification configuration" >&2 
  exit 1
elif [[ $current_config =~ "EventBridgeConfiguration" ]]
then
  echo "- EventBridge notifications already activated"
else
  if [ -z "$current_config" ]
  then
    echo "- No notification configuration, adding EventBridge target"
     new_config='{"EventBridgeConfiguration": {}}'
  else
    echo "- Notification configuration found, adding EventBridge target"
    new_config=$(echo $current_config | jq '. + {EventBridgeConfiguration: {}}')
  fi
  aws s3api --region $1 put-bucket-notification-configuration --bucket $2 --notification-configuration "$new_config"
fi
