
## Ansible Role Test Setup

This folder contains a setup that helps you with testing changes to the Ansible
role for Tenzir.

### Prerequisites

* Podman: Podman makes it easy to run a full operating system in a container
* Ansible: The tool itself to run the test playbook

### Steps

1. Build the container image with `podman build`.
2. Run the example playbook with
   `ansible-playbook -i inventory.ini example-playbook.yaml`.
3. Make a change to the setup such as modifying the `tenzir.yaml` or replacing one
   of the binary packages.
4. Redeploy with the command from step 2.
