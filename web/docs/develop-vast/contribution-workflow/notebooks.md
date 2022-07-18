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

### Add a notebook

To add a new notebook, follow these steps:

1. Place the notebook in the directory [`examples/notebooks`][notebooks].
2. Add potential dependencies that the notebook requires to the Makefile in the
   same directory.
3. Convert the notebook to Markdown via `jupytext --to markdown notebook.ipynb`.
4. Run the notebook as mentioned in the next section.

:::info Markdown Notebooks
All notebooks in our repository are in [Jupytext Markdown][jupytext-markdown]
format. This format is a subset of the Docusaurus Markdown of this
documentation, which makes for a harmonious experience of writing docs and user
guides.
[jupytext-markdown]: https://jupytext.readthedocs.io/en/latest/formats.html#jupytext-markdown
:::

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

Just running `make` executes both steps in order.

## Embed the notebooks in the website

To integrate notebooks including outputs into the website, we leverage the
Markdown conversion from above.

To recreate the outputs from a notebook run with Markdown, go to the `web`
directory and execute the `notebooks` target:

```bash
cd web
make notebooks
```

This triggers the notebook execution as described in the previous section
and copies the output markdown files over to the website directory. You can now
[build the website](documentation) with updated notebooks as usual.
