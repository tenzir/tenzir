---
sidebar_position: 6
---

# Notebooks

We use [Quarto notebooks](https://quarto.org/) as vehicle for self-contained
guides that illustrate how to use VAST.

Quarto notebooks have the file extension `.qmd` and manifest in various places
across the repository:

- `examples/notebooks`: various example notebooks
- `web/blog`: blog posts written as notebook
- `web/docs`: documnetation written as notebook

## Run the notebooks

We leverage Quarto as notebook frontend so that we can run multiple engines,
each of which rely on different kernels. As we use a mix of Bash, Python, and R,
you need the following dependencies to run the notebooks:

- [Quarto](https://quarto.org/docs/get-started/)
- [Poetry](https://python-poetry.org/)
- [R](https://www.r-project.org/)

To render a notebook, run:

```bash
quarto render notebook.qmd
```

Since the `web` directory is a Quarto
[project](https://quarto.org/docs/projects/quarto-projects.html), it suffices
there to run `quarto render` to generate all contained notebooks.

## Run within Docker

We also provide a Docker container to enable a reproducible execution of
notebooks. The container builds on top of the VAST container and adds Quarto,
including all Python and R dependencies. This makes it easy to demonstrate VAST
features within a Quarto notebook.

Other services can be added to the context of the Quarto notebook execution by
extending the Docker Compose setup with [extra
overlays](https://github.com/tenzir/vast/tree/main/docker/).

The website build harness uses this Docker Compose environment to run Quarto
notebooks that represent more elaborate user guides or blog posts that. For
example, running `yarn build` in `/web` compiles the website only after having
executed all notebooks via the Docker Compose environment. Similarly, the
`/examples/notebooks` directory contains example notebooks that leverage this
environment.

To get a shell in this Docker Compose environment, run the following in
`/examples/notebooks` or `/web`:

```bash
make docker TARGET=bash
```

## Add a notebook

The Quarto syntax is a combinatiohn of
[Markdown](https://quarto.org/docs/authoring/markdown-basics.html) and supports
expressing computations in
[Python](https://quarto.org/docs/computations/python.html),
[R](https://quarto.org/docs/computations/r.html), and others. Various [execution
options](https://quarto.org/docs/computations/execution-options.html)
in the YAML frontmatter offer customization on how to run the code.

We chose Quarto as lingua franca for notebooks in this repository, because it
represents a language-agnostic framework with an easy-to-use Markdown syntax.

### Create an example notebook

Adding an example notebook to the repository involves the following steps:

1. Create a new directory in `examples/notebooks` that includes your notebook.

2. Add Python dependencies to `pyproject.toml` file for Poetry.

3. Use `quarto preview` or other subcommands to work with your notebook.

### Create a documentation page

You can use Quarto to write a VAST tutorial or guide in the form as a notebook.
Take a look at the directory `/docs/try` for examples.

Adding a new documentation page involves the following steps:

1. Browse in `web/docs` to the location where you want to add a new page
   `web/blog`.

2. In the directory of your choice, create a file `new-page.qmd`. This is the
   blog post.

3. Use the frontmatter as usual to adjust ordering or perform cosmetic tweaks:

    ```markdown
    ---
    sidebar_position: 42
    ---

    # My New Guide
    ```

4. Write your notebook add Python dependencies into `web/pyproject.toml`
   and R depdencies into `web/DESCRIPTION`.

5. Run `yarn start` and inspect your page locally.

### Create a blog post

Quarto makes it easy to write an entire blog post as a notebook. Take a look at
the directory `/web/blog/a-git-retrospective` for an example.

Writing a new blog post involves the following steps:

1. Create a new directory in `web/blog` that represents your blog post slug,
   e.g., `web/blog/my-blog-post`.

2. In that directory, create a file `index.qmd`. This is the blog post.

3. Add a frontmatter with blog post meta data, e.g.,:

    ```markdown
    ---
    title: My New Blog Post
    authors: mavam
    date: 2042-01-01
    tags: [quarto, notebooks, engineering, open-source]
    ---

    # My Blog Post
    ```

4. Write your blog post and add Python dependencies into `web/pyproject.toml`
   and R depdencies into `web/DESCRIPTION`.

5. Run `yarn start` and inspect the blog post locally.
