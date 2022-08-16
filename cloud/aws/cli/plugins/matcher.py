from typing import Dict, Tuple
from common import TFDIR, FargateService, auto_app_fmt, aws, conf, terraform_output
from vast_invoke import Context, pty_task, task
import plugins.workbucket as workbucket
import core

VALIDATORS = core.VALIDATORS


@pty_task
def deploy(c, auto_approve=False):
    """Deploy the matcher module"""
    core.init_step(c, "matcher")
    c.run(
        f"terragrunt apply {auto_app_fmt(auto_approve)} --terragrunt-working-dir {TFDIR}/matcher",
        env=conf(VALIDATORS),
    )


@pty_task
def destroy(c, auto_approve=False):
    """Remove the matcher module"""
    core.init_step(c, "matcher")
    c.run(
        f"terragrunt destroy {auto_app_fmt(auto_approve)} --terragrunt-working-dir {TFDIR}/matcher",
        env=conf(VALIDATORS),
    )


def service_outputs(c: Context) -> Tuple[str, str, str]:
    cluster = terraform_output(c, "core-2", "fargate_cluster_name")
    family = terraform_output(c, "matcher", "matcher_task_family")
    service_name = terraform_output(c, "matcher", "matcher_service_name")
    return (cluster, service_name, family)


@task
def client_status(c):
    """Get the status of the matcher client"""
    print(FargateService(*service_outputs(c)).get_task_status)


@task
def start_client(c):
    """Start the matcher client instance as an AWS Fargate task. Noop if the client is already running"""
    FargateService(*service_outputs(c)).start_service()


@task
def stop_client(c):
    """Stop the matcher client instance"""
    FargateService(*service_outputs(c)).stop_service()


@task
def restart_client(c):
    """Stop the running matcher client task, the service starts a new one"""
    FargateService(*service_outputs(c)).restart_service()


@task
def post(c):
    queue_url = terraform_output(c, "matcher", "matched_events_queue_url")
    aws("sqs").send_message(
        QueueUrl=queue_url,
        MessageBody="hello",
    )


@task
def attach(c):
    queue_url = terraform_output(c, "matcher", "matched_events_queue_url")
    queue = aws("sqs", resource=True).Queue(queue_url)
    while True:
        messages = queue.receive_messages(VisibilityTimeout=10, WaitTimeSeconds=20)
        for message in messages:
            print(message.body, flush=True)
            message.delete()


MATCHER_INPUT = {
    "feodo": {
        "url": "https://feodotracker.abuse.ch/downloads/ipblocklist.csv",
        "type": "feodo.blocklist",
        "format": "csv",
        "pipe": "tr -d '\015' | grep -v '^#' |",
    },
    "pulsedive": {
        "url": "https://pulsedive.com/premium/?key=&header=true&fields=id,type,risk,threats,feeds,usersubmissions,riskfactors,reference&types=ip,ipv6,domain,url&risk=unknown,none,low,medium,high,critical&period=all&retired=true",
        "type": "pulsedive",
        "format": "csv",
        "predicate": "risk !~ /:retired/ && type == 'ip'",
    },
}


def script(matcher_name: str, input: Dict) -> str:
    return f"""
vast matcher start \
  --mode=exact \
  --match-types=addr \
  {matcher_name} 

curl -sSL "{input["url"]}" |
  {input.get("pipe", "")}
  vast matcher import --type={input["type"]} {input["format"]} {matcher_name} {input.get("predicate", "")}

echo -n "imported lines: "
vast matcher save {matcher_name} | wc -l
"""


@task
def feodo(c):
    core.run_lambda(c, script("feodo", MATCHER_INPUT["feodo"]))


@task
def pulsedive(c):
    core.run_lambda(c, script("pulsedive", MATCHER_INPUT["pulsedive"]))
