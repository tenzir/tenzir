The `python` operator now resolves dependencies with every fresh pipeline run.
Just restart your pipeline to upgrade to the latest available versions of your
Python modules.

The `python` operator no longer uses `pip` but rather
[`uv`](https://github.com/astral-sh/uv). In case you set custom environment
variables for `pip` you need to exchange those with alternative settings that
work with `uv`.
