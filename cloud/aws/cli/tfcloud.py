import requests


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
            ws_for_mod = f"{prefix}-{mod}"
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
    def get_varset(self, name) -> dict:
        "Find a varset from its name, None if doesn't exist"
        res = requests.get(f"{self.org_url}/varsets", headers=self.headers)
        res.raise_for_status()
        return next(
            (vs for vs in res.json()["data"] if vs["attributes"]["name"] == name), None
        )

    @print_error_resp
    def create_varset(self, name, vars: "list[dict]"):
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
                "relationships": {"vars": {"data": [tfvar(**arg) for arg in vars]}},
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
    def assign_varset(self, varset_id, workspace_id):
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
    def set_variable(self, varset_id, variable_id, variable_value):
        payload = {
            "data": {
                "type": "vars",
                "attributes": {
                    "value": variable_value,
                    "category": "env",
                },
            }
        }
        print(f"Assigning value to variable {variable_id}... ", end="")
        res = requests.patch(
            f"{self.url}/varsets/{varset_id}/relationships/vars/{variable_id}",
            headers=self.headers,
            json=payload,
        )
        res.raise_for_status()
        print("DONE")

    @print_error_resp
    def get_variables(self, varset_id):
        res = requests.get(
            f"{self.url}/varsets/{varset_id}/relationships/vars",
            headers=self.headers,
        )
        res.raise_for_status()
        return res.json()["data"]
