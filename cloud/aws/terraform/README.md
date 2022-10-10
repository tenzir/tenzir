# Infrastructure deployment scripts

Infrastructure is managed by Terraform.

## Terragrunt

We use Terragrunt to
- DRY the Terraform config
- Manage dependencies between modules

## Managing resource name uniqueness

Some Terraform resources require names that are unique within a region. The
common pattern is to concatenate identifiers of the module and its context to
provide this uniqueness:

```
id_raw = "${var.source_name}-${var.source_bucket_name}-${module.env.module_name}-${module.env.stage}"
```

The issue with this approach is that name become excessively long and sometimes
exceed naming limitations. To overcome this issue, we hash the identifiers and
take only the first characters of the hash. 6 hexa digits should be more than
sufficient to avoid conflicts as this stack will be deployed only a very
moderate amount of times within an account.

```
id = substr(md5(local.id_raw), 0, 6)
```

## Assigning default values

Dependent module outputs and `get_env` calls in the Terragrunt module files
(`terragrunt.hcl`) should always have default values to avoid errors when
the stack is called without these modules. This is mostly due to some bugs in
how Terragrunt initializes modules.
