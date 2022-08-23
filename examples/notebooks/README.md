# Notebooks

We use [Jupyter notebooks](https://jupyter.org/) as vehicle for self-contained
guides that illustrate how to use VAST.

Our repositories contains a few examples in the directory
[`examples/notebooks`][notebooks]. These notebooks (will) run in CI and
produce outputs as side effect, which we capture and include in the
documentation under the section [Try VAST](/docs/try-vast).

[notebooks]: https://github.com/tenzir/vast/tree/master/examples/notebooks

## Run the notebooks

Our notebook stack consists of Jupyter, [jupytext][jupytext], and
[nbconvert][nbconvert] to run notebooks and convert their outputs.

[jupytext]: https://github.com/mwouts/jupytext
[nbconvert]: https://nbconvert.readthedocs.io/en/latest/

The execution of a notebook is a two-step process:

1. Run then notebook and capture outputs in the correpsponding *.ipynb file.
2. Translate the *.ipynb file to Markdown, suitable for embedding in the docs.

Ideally, we would go directly to the Markdown output for the docs, but jupytext
only captures outputs when converting to `*.ipynb`. So we use nbconvert
afterwards to translate the `*.ipynb` files to Markdown.

The Makefile in [`examples/notebooks`][notebooks] relies  up Python virtual
environment with all needed dependencies. To run the notebooks and capture their
output, use the `ipynb` target:

Make sure your have [Poetry](https://python-poetry.org/) installed, which we use
for Python dependency management. To execute the above steps in sequence, enter
the `examples/notebooks` directory and run:

```bash
make
```

You can also trigger the phases manually. First translate to `*.ipynb`:

```bash
make ipynb
```

Thereafter, convert to Markdown:

```bash
make md
```

### Add a notebook

:::info Markdown Notebooks
All notebooks in our repository are in [Jupytext Markdown][jupytext-markdown]
format. This format is a subset of the Docusaurus Markdown we use in our
documentation, which makes for a harmonious experience of writing docs and user
guides.
:::

[jupytext-markdown]: https://jupytext.readthedocs.io/en/latest/formats.html#jupytext-markdown

To add a new notebook, follow these steps:

1. Place the notebook in the directory [`examples/notebooks`][notebooks].
2. Add potential dependencies in the `pyproject.toml` file for Poetry.
3. Run the notebook as mentioned in the above section.

## Embed the notebooks in the website

To integrate notebooks including outputs into the website, we second step of the
above conversion.

To recreate the outputs from a notebook run with Markdown, go to the `web`
directory and execute the `notebooks` target:

```bash
cd web
make notebooks
```

This triggers the notebook execution as described in the previous section
and copies the output markdown files over to the website directory. You can now
[build the website](documentation) with updated notebooks as usual.
