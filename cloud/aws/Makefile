-include default.env

# these are the variables you can set in the default.env file
peered_vpc_id ?= $(eval peered_vpc_id := $(shell bash -c 'read -p "Peered VPC ID (existing): " input; echo $$input'))$(peered_vpc_id)
vast_cidr ?= $(eval vast_cidr := $(shell bash -c 'read -p "VAST CIDR (new VPC): " input; echo $$input'))$(vast_cidr)
aws_region ?= $(eval aws_region := $(shell bash -c 'read -p "AWS Region: " input; echo $$input'))$(aws_region)
vast_version ?= $(shell git describe --abbrev=0 --match='v[0-9]*' --exclude='*-rc*')
vast_server_storage_type ?= EFS

# re-export these variable into the environement for Terraform and the AWS CLI to use
export AWS_REGION = ${aws_region}
export TF_VAR_peered_vpc_id = ${peered_vpc_id}
export TF_VAR_vast_cidr = ${vast_cidr}
export TF_VAR_region_name = ${aws_region}
export TF_VAR_vast_version = ${vast_version}
export TF_VAR_vast_server_storage_type = ${vast_server_storage_type}

help:     ## Show this help.
	@egrep -h '\s##\s' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m  %-30s\033[0m %s\n", $$1, $$2}'

init:
	terraform init

docker-login:
	aws ecr get-login-password | \
	docker login --username AWS --password-stdin \
		"$(shell aws sts get-caller-identity --query 'Account' --output text).dkr.ecr.${aws_region}.amazonaws.com"

docker-build:
	docker build --build-arg VAST_VERSION=${vast_version} -t tenzir/vast-lambda:${vast_version} .

apply:
	terraform apply

deploy: init docker-login docker-build apply ## One liner build and deploy of the stack to AWS

destroy: stop-all-tasks ## Tear down the entire terraform stack
	terraform destroy

run-vast-task:
	@aws ecs run-task \
		--cluster $(shell terraform output fargate_cluster_name) \
		--count 1 \
		--enable-ecs-managed-tags \
		--enable-execute-command \
		--propagate-tags TASK_DEFINITION \
		--launch-type FARGATE  \
		--network-configuration "awsvpcConfiguration={subnets=[$(shell terraform output ids_appliances_subnet_id)],securityGroups=[$(shell terraform output vast_security_group)]}" \
		--task-definition $(shell terraform output vast_task_definition) \
	| jq -r '.tasks[].taskArn? | split("/")[-1]' \
	| xargs -I _ echo "Started task _" 



start-vast-server: ## Start a VAST server instance as an AWS Fargate task. Noop if a VAST server is already running.
	@if [ -n "$(shell $(MAKE) get-vast-server)" ]; then  \
		echo "VAST server already running"; exit 1; \
	fi
	@$(MAKE) --no-print-directory run-vast-task 

restart-vast-server: ## Stops all running VAST server Fargate tasks and start a new one.
	@for task in $(shell $(MAKE) get-vast-server); do \
		aws ecs stop-task --task $$task --cluster $(shell terraform output fargate_cluster_name) > /dev/null; \
		echo "Stopped task $$task"; \
	done
	@$(MAKE) --no-print-directory run-vast-task

# There should be only one VAST server running at once, but if we accidentally have multiple ones, this will list them all
get-vast-server: ## List the running VAST server task id(s) on the ECS cluster created by Terraform
	@aws ecs list-tasks --family $(shell terraform output vast_task_family) --cluster $(shell terraform output fargate_cluster_name) | jq -r '.taskArns | map(split("/")[-1]) | reduce .[] as $$item (""; . + $$item + " ")'

get-vast-server-ip:
	@aws ecs describe-tasks \
		--cluster $(shell terraform output fargate_cluster_name) \
		--task $(shell $(MAKE) get-vast-server) | jq -r '.. | .privateIpv4Address? | select(. != null)'

list-all-tasks: ## List the ids of all tasks running on the ECS cluster
	@aws ecs list-tasks --cluster $(shell terraform output fargate_cluster_name) | jq -r '.taskArns | map(split("/")[-1]) | reduce .[] as $$item (""; . + $$item + " ")'

stop-all-tasks: ## Stop all running tasks on the ECS cluster created by Terraform
	@for task in $(shell $(MAKE) list-all-tasks); do \
		aws ecs stop-task --task $$task --cluster $(shell terraform output fargate_cluster_name) > /dev/null; \
		echo "Stopped task $$task"; \
	done

# Provide your bash command through the CMD variable (e.g make run-lambda CMD="vast status")
run-lambda: ## Run ad-hoc VAST client commands from AWS Lambda
	@$(file > cmd.tmp,$(CMD)) # write to file first and encode to avoid escaping issues
	@aws lambda invoke \
		--function-name $(shell terraform output vast_lambda_name) \
		--cli-binary-format raw-in-base64-out \
		--payload '{"cmd":"$(shell base64 cmd.tmp)", "host":"$(shell $(MAKE) get-vast-server-ip):42000"}' \
		--cli-read-timeout 0 \
		/dev/stdout | jq -s -r .[0].result
	@rm cmd.tmp

# Provide your bash command through the CMD variable (e.g make execute-command CMD="vast status")
# Start an interactive session by leaving CMD empty
execute-command: ## Run ad-hoc or interactive commands from the VAST server Fargate task
	@$(file > cmd.tmp,$(CMD)) # write to file first and encode to avoid escaping issues
	@aws ecs execute-command \
		--cluster $(shell terraform output fargate_cluster_name) \
		--task $(shell $(MAKE) get-vast-server) \
		--interactive \
		--command "$(shell if [ -z "${CMD}" ]; then echo "/bin/bash"; else echo "/bin/bash -c 'echo $(shell base64 -w 0 cmd.tmp) | base64 -d | /bin/bash'"; fi)"
	@rm cmd.tmp
