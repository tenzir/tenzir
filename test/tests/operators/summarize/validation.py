import os
import shlex
import subprocess


BINARY = [*shlex.split(os.environ["TENZIR_BINARY"]), "--bare-mode", "--nova=true"]


def compile_pipeline(pipeline, stage="--dump-ir-plan"):
    return subprocess.run(
        [*BINARY, stage, pipeline],
        capture_output=True,
        text=True,
        timeout=10,
    )


# Planning must resolve let-bound constants without executing the pipeline.
for pipeline in (
    "let $normalize = true\nfrom []\nsummarize result=entropy(x, normalize=$normalize)",
    "let $normalize = true\nfrom []\n"
    "if false { summarize result=entropy(x, normalize=$normalize) }",
):
    result = compile_pipeline(pipeline)
    assert result.returncode == 0, result.stderr
    assert not result.stderr, result.stderr
    assert "summarize" in result.stdout, result.stdout

# Invalid calls must fail during planning, even with no input or an unused branch.
for pipeline, error in (
    ("from []\nsummarize result=sum()", "expected exactly 1 positional argument"),
    (
        "let $normalize = 42\nfrom []\nsummarize result=entropy(x, normalize=$normalize)",
        "expected argument of type `bool`, but got `int`",
    ),
    (
        "from []\nif false { summarize result=sum() }",
        "expected exactly 1 positional argument",
    ),
):
    result = compile_pipeline(pipeline)
    assert result.returncode != 0, result.stdout
    assert error in result.stderr, result.stderr
    assert not result.stdout, result.stdout

# Representation support is checked by type inference, before plan construction.
result = compile_pipeline(
    "from []\nsummarize result=frequency_table(x)", "--dump-opt-ir"
)
assert result.returncode != 0, result.stdout
assert "`frequency_table` does not support `--nova` yet" in result.stderr, result.stderr

print("aggregation planning and validation: ok")
