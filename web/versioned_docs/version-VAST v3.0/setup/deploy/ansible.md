---
sidebar_position: 3
---

# Ansible

The Ansible role for VAST allows for easy integration of VAST into
existing Ansible setups. The role uses either the VAST Debian package or
the tarball installation method depending on which is appropriate for the
target environment.
The role definition is in the [`ansible/roles/vast`][vast-repo-ansible]
directory of the VAST repository. You need a local copy of this directory so you
can use it in your playbook.

[vast-repo-ansible]: https://github.com/tenzir/vast/tree/main/ansible/roles/vast

## Example

This example playbook shows how to run a VAST service on the machine
`example_vast_server`:

```yaml
- name: Deploy vast
  become: true
  hosts: example_vast_server
  remote_user: example_ansible_user
  roles:
    - role: vast
      vars:
        config_file: ./vast.yaml
        read_write_paths: [ /tmp ]
        vast_archive: ./vast.tar.gz
        vast_debian_package: ./vast.deb
```

## Variables

### `config_file` (required)

A path to a [`vast.yaml`](../configure.md#configuration-files) relative to
the playbook.

### `read_write_paths`

A list of paths that VAST shall be granted access to in addition to its own
state and log directories.

### `vast_archive`

A tarball of VAST structured like those that can be downloaded from the [GitHub
Releases Page](https://github.com/tenzir/vast/releases). This is used for target
distributions that are not based on the `apt` package manager.

### `vast_debian_package`

A Debian package (`.deb`). This package is used for Debian and Debian based
Linux distributions.
