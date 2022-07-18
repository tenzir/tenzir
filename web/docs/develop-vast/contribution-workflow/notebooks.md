# Notebooks

We use [Jupyter notebooks](https://jupyter.org/) as vehicle for self-contained
guides that illustrate how to use VAST. These notebooks (will) run in CI and
produce outputs as side effect, which we capture and include in the
documentation under the section
[Try VAST](/docs/try-vast).

:::tip TL;DR
If you just want to make sure the notebooks still work after having made any
changes, it suffices to invoke `make` in the directory
[`examples/notebooks`][notebooks].
:::

[notebooks]: https://github.com/tenzir/vast/tree/master/examples/notebooks

### Run the notebooks

Our notebook stack consists of Jupyter, [jupytext][jupytext], and
[nbconvert][nbconvert] to run notebooks and convert their outputs.

[jupytext]: https://github.com/mwouts/jupytext
[nbconvert]: https://nbconvert.readthedocs.io/en/latest/

The Makefile in [`examples/notebooks`][notebooks] sets up Python virtual
environment with all needed dependencies. To run the notebooks and capture their
output, use the `ipynb` target:

```bash
cd examples/notebooks
make ipynb
```

Thereafter, you can convert the notebook with outputs into Markdown:

```bash
make md
```

Just running `make` performs both steps in order.

## Embedding notebooks

To integrate notebooks including outputs into the website, we leverage the
Markdown conversion from above.

To recreate the outputs from a notebook run with Markdown, go to the `web`
directory and execute the `notebooks` target:

```bash
cd web
make notebooks
```

This triggers the notebook execution as described in the previous section. You
can now [build the website](documentation) with updated notebooks as usual.
