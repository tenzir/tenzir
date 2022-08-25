from typing import Dict
from vast_invoke import task
import plugins.workbucket as workbucket
import core

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


@task
def attach(c, matcher_name, output_type="csv"):
    core.execute_command(
        c,
        f"vast matcher attach {output_type} {matcher_name}",
    )


@task
def save(c, matcher_name, workbucket_key):
    core.run_lambda(
        c,
        f"vast matcher save {matcher_name} | aws s3 cp - s3://{workbucket.name(c)}/{workbucket_key}",
    )


@task
def load(c, matcher_name, workbucket_key):
    core.run_lambda(
        c,
        f"aws s3 cp s3://{workbucket.name(c)}/{workbucket_key} - | vast matcher load {matcher_name}",
    )
