---
sidebar_position: 3
---

# Ansible

The Ansible role for Tenzir allows for easy integration of Tenzir into
existing Ansible setups. The role uses either the Tenzir Debian package or
the tarball installation method depending on which is appropriate for the
target environment. The role definition is in the
[`ansible/roles/tenzir`][tenzir-repo-ansible] directory of the Tenzir
repository. You need a local copy of this directory so you can use it in your
playbook.

[tenzir-repo-ansible]: https://github.com/tenzir/tenzir/tree/main/ansible/roles/tenzir

## Example

This example playbook shows how to run a Tenzir service on the machine
`example_tenzir_server`:

```yaml
- name: Deploy Tenzir
  become: true
  hosts: example_tenzir_server
  remote_user: example_ansible_user
  roles:
    - role: tenzir
      vars:
        config_file: ./tenzir.yaml
        read_write_paths: [ /tmp ]
        tenzir_archive: ./tenzir.tar.gz
        tenzir_debian_package: ./tenzir.deb
```

## Variables

### `config_file` (required)

A path to a [`tenzir.yaml`](../configure.md#configuration-files) relative to
the playbook.

### `read_write_paths`

A list of paths that Tenzir shall be granted access to in addition to its own
state and log directories.

### `tenzir_archive`

A tarball of Tenzir structured like those that can be downloaded from the
[GitHub Releases Page](https://github.com/tenzir/tenzir/releases). This is used
for target distributions that are not based on the `apt` package manager.

### `tenzir_debian_package`

A Debian package (`.deb`). This package is used for Debian and Debian based
Linux distributions.
