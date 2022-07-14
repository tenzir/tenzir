# Notebooks

We use [Jupyter notebooks](https://jupyter.org/) as vehicle for self-contained
guides of using VAST. These notebooks (will) run in CI and produce outputs as
side effect, which we capture and include in the documentation under the section
[Try VAST](/docs/try-vast).

:::tip TL;DR
If you just want to make sure the notebooks still work after having made any
changes, it suffices to invoke `make` in the `web` directory.
:::

### Setup local environment

We use a Python virtual env that contains the tooling needed to run the
notebooks and convert them. This happens in the context of building the site,
which is why the scaffolding is in the `web` directory. We use Make to automate
the process:

```bash
cd web
# Setup the Python virtual env.
make env
```

Thereafter, you can use other Make targets to work with the notebooks.

## Running notebooks

All notebooks that show up on the site serve as independent examples, and are
therefore located in the directory [examples/notebooks][examples] in the
top-level of the repository.

[examples]: https://github.com/tenzir/vast/tree/master/examples/notebooks

The notebooks do not come with outputs, i.e., they just contain the commands to
run. This is by design so that CI must first successfully execute the notebook
prior to green-lighting a change.

:::caution TODO
Notebook execution is not yet implemented.
:::

## Embedding notebooks

We use [nbconvert](https://nbconvert.readthedocs.io/en/latest/) to convert the
native Jupyter notebook into Markdown, which Docusaurus then integrates
seamlessly into the site.

This happens automatically when you run `yarn start` to [build the
website](documentation).
