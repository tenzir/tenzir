-include default.env

vpc_id ?= $(eval vpc_id := $(shell bash -c 'read -p "VPC ID (existing): " input; echo $$input'))$(vpc_id)
subnet_cidr ?= $(eval subnet_cidr := $(shell bash -c 'read -p "Subnet CIDR (to be created): " input; echo $$input'))$(subnet_cidr)
aws_region ?= $(eval aws_region := $(shell bash -c 'read -p "AWS Region: " input; echo $$input'))$(aws_region)

init:
	terraform init

docker-login:
	aws ecr get-login-password --region "${aws_region}" | \
	docker login --username AWS --password-stdin \
		"$(shell aws sts get-caller-identity --query 'Account' --output text).dkr.ecr.${aws_region}.amazonaws.com"

docker-build:
	docker build -t tenzir/vast-lambda:latest .

apply:
	terraform apply -var="region_name=${aws_region}" -var="vpc_id=${vpc_id}" -var="subnet_cidr=${subnet_cidr}"

deploy: init docker-login docker-build apply

destroy:
	terraform destroy -var="region_name=${aws_region}" -var="vpc_id=${vpc_id}" -var="subnet_cidr=${subnet_cidr}"

run-task:
	aws ecs run-task \
		--cluster $(shell terraform output fargate_cluster_name) \
		--region ${aws_region} \
		--count 1 \
		--enable-ecs-managed-tags \
		--enable-execute-command \
		--propagate-tags TASK_DEFINITION \
		--launch-type FARGATE  \
		--network-configuration awsvpcConfiguration={subnets=[$(shell terraform output ids_subnet_id)],securityGroups=[$(shell terraform output vast_security_group)],assignPublicIp=ENABLED} \
		--task-definition $(shell terraform output vast_task_definition)

list-tasks:
	@aws --region ${aws_region} ecs list-tasks --cluster $(shell terraform output fargate_cluster_name) | jq -r '.taskArns | map(.[64:]) | reduce .[] as $$item (""; . + $$item + " ")'

stop-all-tasks:
	for task in $(shell $(MAKE) list-tasks); do \
		aws --region ${aws_region} ecs stop-task --task $$task --cluster $(shell terraform output fargate_cluster_name); \
	done

get-task-ip:
	@aws ecs describe-tasks --cluster vast-cluster-default --task $(shell $(MAKE) list-tasks) | jq -r '.. | .privateIpv4Address? | select(. != null)'

# use | jq -r ".result" for a formatted output
run-lambda:
	@aws lambda invoke \
		--function-name $(shell terraform output vast_lambda_name) \
		--cli-binary-format raw-in-base64-out \
		--region ${aws_region} \
		--payload '{"cmd":"$(CMD)", "host":"$(shell $(MAKE) get-task-ip):42000"}' \
		lambda-result.json > /dev/null && \
	cat lambda-result.json
