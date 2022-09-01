"""Configure the Terraform Cloud account used as Terraform state backend"""

from vast_invoke import task
import dynaconf
import requests
from common import conf, list_modules, tf_version

VALIDATORS = [
    dynaconf.Validator("TF_ORGANIZATION", must_exist=True, ne=""),
    dynaconf.Validator("TF_API_TOKEN", must_exist=True, ne=""),
]


def tfvar(key: str, sensitive: bool):
    """Creates a payload for a variable object"""
    return {
        "type": "vars",
        "attributes": {
            "key": key,
            "value": "",
            "category": "env",
            "sensitive": sensitive,
        },
    }


def print_error_resp(func):
    """An anotation that helps undestanding request errors"""

    def func_wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except requests.RequestException as e:
            print(e.response.text)
            raise Exception(e)

    return func_wrapper


class Client:
    """A client with useful requests to the Terraform Cloud API"""

    def __init__(self, org: str, token: str):
        self.url = "https://app.terraform.io/api/v2"
        self.org_url = f"{self.url}/organizations/{org}"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "content-type": "application/vnd.api+json",
        }

    @print_error_resp
    def list_workspaces(self, prefix: str):
        res = requests.get(
            f"{self.org_url}/workspaces?search%5Bname%5D={prefix}",
            headers=self.headers,
        )
        res.raise_for_status()
        ws_ls = res.json()["data"]
        ws_map = {ws["attributes"]["name"]: ws for ws in ws_ls}
        return ws_map

    @print_error_resp
    def upsert_workspaces(
        self, prefix: str, modules: "list[str]", tf_version: str, tf_work_dir: str
    ) -> "list[dict]":
        ws_map = self.list_workspaces(prefix)
        updted_ws_list = []
        for mod in modules:
            ws_for_mod = f"{prefix}{mod}"
            payload = {
                "data": {
                    "attributes": {
                        "name": ws_for_mod,
                        "terraform-version": tf_version,
                        "working-directory": f"{tf_work_dir}/{mod}",
                        "execution-mode": "remote",
                        "tag-names": ["vast"],
                    },
                    "type": "workspaces",
                }
            }
            if ws_for_mod in ws_map:
                print(f"Updating workspace {ws_for_mod}... ", end="")
                res = requests.patch(
                    f"{self.org_url}/workspaces/{ws_for_mod}",
                    headers=self.headers,
                    json=payload,
                )
            else:
                print(f"Creating workspace {ws_for_mod}...", end="")
                res = requests.post(
                    f"{self.org_url}/workspaces",
                    headers=self.headers,
                    json=payload,
                )
            res.raise_for_status()
            res_data = res.json()["data"]
            print(f"DONE ({res_data['id']})")
            updted_ws_list.append(res.json()["data"])
        return updted_ws_list

    @print_error_resp
    def get_varset(self, name: str) -> dict:
        "Find a varset from its name, None if doesn't exist"
        res = requests.get(f"{self.org_url}/varsets", headers=self.headers)
        res.raise_for_status()
        return next(
            (vs for vs in res.json()["data"] if vs["attributes"]["name"] == name), None
        )

    @print_error_resp
    def create_varset(self, name: str) -> dict:
        existing_varset = self.get_varset(name)
        if existing_varset is not None:
            print(f"Varset {name} already exists ({existing_varset['id']})")
            return existing_varset
        payload = {
            "data": {
                "type": "varsets",
                "attributes": {
                    "name": name,
                    "global": False,
                },
            }
        }
        print(f"Creating varset {name}... ", end="")
        res = requests.post(
            f"{self.org_url}/varsets", headers=self.headers, json=payload
        )
        res.raise_for_status()
        existing_varset = res.json()["data"]
        print(f"DONE ({existing_varset['id']})")
        return existing_varset

    @print_error_resp
    def assign_varset(self, varset_id: str, workspace_id: str):
        payload = {
            "data": [
                {
                    "type": "workspaces",
                    "id": workspace_id,
                }
            ]
        }
        print(f"Assigning varset {varset_id} to workspace {workspace_id}... ", end="")
        res = requests.post(
            f"{self.url}/varsets/{varset_id}/relationships/workspaces",
            headers=self.headers,
            json=payload,
        )
        res.raise_for_status()
        print("DONE")

    @print_error_resp
    def get_variable(self, varset_id: str, key: str) -> dict:
        "Find a variable in a varset by its key, None if doesn't exist"
        res = requests.get(
            f"{self.url}/varsets/{varset_id}/relationships/vars",
            headers=self.headers,
        )
        res.raise_for_status()
        return next(
            (var for var in res.json()["data"] if var["attributes"]["key"] == key),
            None,
        )

    @print_error_resp
    def set_variable(
        self, varset_id: str, variable_key: str, variable_value: str, sensitive: bool
    ):
        payload = {
            "data": {
                "type": "vars",
                "attributes": {
                    "key": variable_key,
                    "value": variable_value,
                    "category": "env",
                    "sensitive": sensitive,
                },
            }
        }
        var = self.get_variable(varset_id, variable_key)
        if var is not None:
            print(f"Updating variable {variable_key} in set {varset_id}... ", end="")
            res = requests.patch(
                f"{self.url}/varsets/{varset_id}/relationships/vars/{var['id']}",
                headers=self.headers,
                json=payload,
            )
        else:
            print(f"Creating variable {variable_key} in set {varset_id}... ", end="")
            res = requests.post(
                f"{self.url}/varsets/{varset_id}/relationships/vars",
                headers=self.headers,
                json=payload,
            )
        res.raise_for_status()
        print("DONE")


@task(
    help={
        "auto": """if set to True, this will forward the values of your 
current environement variables. Otherwise you will be prompted for 
the values you want to give to the environment variables"""
    }
)
def config(c, auto=False):
    """Configure workspaces in your Terrraform Cloud account"""
    config = conf(VALIDATORS)
    client = Client(
        config["TF_ORGANIZATION"],
        config["TF_API_TOKEN"],
    )
    ws_list = client.upsert_workspaces(
        config["TF_WORKSPACE_PREFIX"],
        list_modules(c),
        tf_version(c),
        "cloud/aws/terraform",
    )

    varset = client.create_varset(
        f"{config['TF_WORKSPACE_PREFIX']}aws-creds",
    )
    for ws in ws_list:
        client.assign_varset(varset["id"], ws["id"])

    var_defs = [
        {"key": "AWS_SECRET_ACCESS_KEY", "sensitive": True},
        {"key": "AWS_ACCESS_KEY_ID", "sensitive": False},
    ]
    for var_def in var_defs:
        if auto:
            value = config[var_def["key"]]
        else:
            value = input(f"{var_def['key']} (Ctrl+c to cancel):")
        client.set_variable(varset["id"], var_def["key"], value, var_def["sensitive"])
