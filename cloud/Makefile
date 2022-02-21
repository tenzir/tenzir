-include default.env

vpc_id ?= $(eval vpc_id := $(shell bash -c 'read -p "VPC ID (existing): " input; echo $$input'))$(vpc_id)
subnet_cidr ?= $(eval subnet_cidr := $(shell bash -c 'read -p "Subnet CIDR (to be created): " input; echo $$input'))$(subnet_cidr)
aws_region ?= $(eval aws_region := $(shell bash -c 'read -p "AWS Region: " input; echo $$input'))$(aws_region)

init:
	terraform init

apply:
	terraform apply -var="region_name=${aws_region}" -var="vpc_id=${vpc_id}" -var="subnet_cidr=${subnet_cidr}"

deploy: init apply

destroy:
	terraform destroy -var="region_name=${aws_region}" -var="vpc_id=${vpc_id}" -var="subnet_cidr=${subnet_cidr}"

run-task:
	aws ecs run-task \
		--cluster $(shell bash -c '${terraform} output fargate_cluster_name') \
		--region ${aws_region} \
		--count 1 \
		--enable-ecs-managed-tags \
		--propagate-tags TASK_DEFINITION \
		--launch-type FARGATE  \
		--network-configuration awsvpcConfiguration={subnets=[$(shell bash -c '${terraform} output ids_subnet_id')],securityGroups=[$(shell bash -c '${terraform} output vast_security_group')],assignPublicIp=ENABLED} \
		--task-definition $(shell bash -c '${terraform} output vast_task_definition')

list-tasks:
	aws --region ${aws_region} ecs list-tasks --cluster $(shell bash -c 'terraform output fargate_cluster_name') | jq -r '.taskArns | map(.[64:]) | reduce .[] as $$item (""; . + $$item + " ")'

stop-all-tasks:
	for task in $(shell bash -c 'make list-tasks'); do \
		aws --region ${aws_region} ecs stop-task --task $$task --cluster $(shell bash -c 'terraform output fargate_cluster_name'); \
	done

