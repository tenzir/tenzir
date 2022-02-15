## NixOps VAST deployment example

To start, copy the this directory to your local filesystem.
Then start a development shell with the necessary dependencies:

```bash
nix develop
```

Inside that shell create a nixops network and deploy a machine running VAST:

```bash
nixops create -d vast --flake .
nixops deploy -d vast
# nixops should now provision a VirtualBox VM and deploy the system
# configuration.
```
---
**NOTE**
In case the VAST process aborts with an illegal instruction signal you need
to change the virtualization method for the VM to "default".

Then you can restart the machine and the service should be online.
---

When you're done you can stop the system and delete the created resources:

```bash
nixops stop -d vast
nixops delete-resources -d vast
nixops delete -d vast
```
