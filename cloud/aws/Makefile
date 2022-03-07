-include default.env

vpc_id ?= $(eval vpc_id := $(shell bash -c 'read -p "VPC ID (existing): " input; echo $$input'))$(vpc_id)
subnet_cidr ?= $(eval subnet_cidr := $(shell bash -c 'read -p "Subnet CIDR (to be created): " input; echo $$input'))$(subnet_cidr)
aws_region ?= $(eval aws_region := $(shell bash -c 'read -p "AWS Region: " input; echo $$input'))$(aws_region)

export AWS_REGION = ${aws_region}

help:     ## Show this help.
	@egrep -h '\s##\s' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m  %-30s\033[0m %s\n", $$1, $$2}'

init:
	terraform init

docker-login:
	aws ecr get-login-password | \
	docker login --username AWS --password-stdin \
		"$(shell aws sts get-caller-identity --query 'Account' --output text).dkr.ecr.${aws_region}.amazonaws.com"

docker-build:
	docker build -t tenzir/vast-lambda:latest .

apply:
	terraform apply -var="region_name=${aws_region}" -var="vpc_id=${vpc_id}" -var="subnet_cidr=${subnet_cidr}"

deploy: init docker-login docker-build apply ## One liner that builds and deploys the stack to AWS

destroy: stop-all-tasks
	terraform destroy -var="region_name=${aws_region}" -var="vpc_id=${vpc_id}" -var="subnet_cidr=${subnet_cidr}"

run-task: ## Start a VAST server instance as an AWS Fargate task
	aws ecs run-task \
		--cluster $(shell terraform output fargate_cluster_name) \
		--count 1 \
		--enable-ecs-managed-tags \
		--enable-execute-command \
		--propagate-tags TASK_DEFINITION \
		--launch-type FARGATE  \
		--network-configuration "awsvpcConfiguration={subnets=[$(shell terraform output ids_appliances_subnet_id)],securityGroups=[$(shell terraform output vast_security_group)]}" \
		--task-definition $(shell terraform output vast_task_definition)

list-tasks: ## List all running tasks on the ECS cluster created by Terraform
	@aws ecs list-tasks --cluster $(shell terraform output fargate_cluster_name) | jq -r '.taskArns | map(split("/")[-1]) | reduce .[] as $$item (""; . + $$item + " ")'

stop-all-tasks: ## Stop all running tasks on the ECS cluster created by Terraform
	for task in $(shell $(MAKE) list-tasks); do \
		aws ecs stop-task --task $$task --cluster $(shell terraform output fargate_cluster_name); \
	done

get-task-ip:
	@aws ecs describe-tasks \
		--cluster vast-cluster-default \
		--task $(shell $(MAKE) list-tasks) | jq -r '.. | .privateIpv4Address? | select(. != null)'

# Provide your bash command through the CMD variable (e.g make run-lambda CMD="vast status")
# Use | jq -r ".result" for a formated output
# Inside the CMD variable, escape:
# - " with \\\"
# - ' with '\''
# TODO automatic escaping
run-lambda: ## Run ad-hoc VAST clients from AWS Lambda
	@aws lambda invoke \
		--function-name $(shell terraform output vast_lambda_name) \
		--cli-binary-format raw-in-base64-out \
		--payload '{"cmd":"$(CMD)", "host":"$(shell $(MAKE) get-task-ip):42000"}' \
		/dev/stdout | jq -s -r .[0].result

execute-command: ## Start an interactive bash shell within the Fargate container
	aws ecs execute-command \
		--cluster $(shell terraform output fargate_cluster_name) \
		--task $(shell $(MAKE) list-tasks) \
		--interactive \
		--command /bin/bash
