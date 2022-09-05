import os
import subprocess
import boto3

AWS_REGION = os.getenv("AWS_REGION")
client = boto3.client("servicediscovery", region_name=AWS_REGION)

cloudflared_dir = f"{os.environ['HOME']}/.cloudflared"

os.makedirs(cloudflared_dir, exist_ok=True)


# response = client.list_services(
#     Filters=[
#         {
#             "Name": "NAMESPACE_ID",
#             "Values": [os.environ["NAMESPACE_ID"]],
#         }
#     ]
# )
# print({srv["Name"] for srv in response["Services"]})


config = f"""tunnel: {os.environ["CF_TUNNEL_ID"]}
logfile: /dev/null
loglevel: info

ingress:
  - hostname: "*"
    service: http://misp.94aaa7.vast.local:80
"""
config_file = f"{cloudflared_dir}/config.yml"
with open(config_file, "w") as f:
    f.write(config)

popen = subprocess.Popen(
    [
        "cloudflared",
        "tunnel",
        "run",
        # "--token",
        # os.environ["CF_SECRET"],
        # "--credentials-contents",
        # "{}",
    ]
)
return_code = popen.wait()
print(f"Cloudflared returned with exit code {return_code}")
exit(1)
