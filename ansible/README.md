This directory contains an Ansible host role for VAST.
The role can be parameterized with three variables:

* `config_file`: A path a `vast.yaml` relative to the playbook (required)
* `read_write_paths`: A list of paths that vast shall be granted access to
  in addition to its own state and log directories.
* `vast_archive`: A tarball of vast structured like those that can be downloaded
  from the [GitHub Releases Page](https://github.com/tenzir/vast/releases).
  This is used for target distributions that are not based on the `apt`
  package manager.
* `vast_debian_package`: A Debian package (`.deb`). This package is used
  for Debian and Debian based Linux distributions.

#### Example

```
- name: Deploy vast
  become: true
  hosts: example
  remote_user: example_ansible_user
  roles:
    - role: vast
      vars:
        config_file: ./vast.yaml
        read_write_paths: [ /tmp ]
        vast_debian_package: ./vast.deb
```
