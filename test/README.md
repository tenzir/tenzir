# Tenzir Tests

This directory hosts the Tenzir integration tests.

## 🚧 Refactoring Plan

We are in the middle of migrating /tenzir/tests/ to /test/.

This takes place in three phases:

### Phase 1: Migration of existing tests

We begin with incrementally moving all directories in /tenzir/tests/* over to
/test/. We started already with git-mv'ing the diretories:

- /tenzir/tests/exec
- /tenzir/tests/inputs

As we go through the directories, we want to perform one commit per directory.
This is important so that we can move along incrementally.

#### Runner migration

During this effort, we are going to migrate the following runners from the
tenzir-test repo into /test/runners:

| Runner          | Command                                          | Input Extension  | Artifact |
| --------------- | ------------------------------------------------ | ---------------- | -------- |
| `lexer`         | `tenzir --dump-tokens -f <test>`                 | `.tql`           | `.txt`   |
| `ast`           | `tenzir --dump-ast -f <test>`                    | `.tql`           | `.txt`   |
| `ir`            | `tenzir --dump-ir -f <test>`                     | `.tql`           | `.txt`   |
| `finalize`      | `tenzir --dump-finalized -f <test>`              | `.tql`           | `.txt`   |
| `instantiation` | `tenzir --dump-ir` ⇄ `tenzir --dump-inst-ir`     | `.tql`           | `.diff`  |
| `opt`           | `tenzir --dump-inst-ir` ⇄ `tenzir --dump-opt-ir` | `.tql`           | `.diff`  |

Basically all runners except for the `tenzir` runner will be migrated.

#### Runner configuration mechanism

We should _consider_ introducing a new mechanism to configure `tenzir-test` for
a given directory plus all children. The problem we are encountering is that
it's not sufficient to just have a runner registration mechanism that's
extension based. We want to select the runner based on the name of a given
directory.

To this end, let's consider the following: a file `test.yaml` that is equivalent
in semantics to the YAML frontmatter that all files can have. If `test.yaml`
exists in a given directory, the provided key-value pairs should become the new
default for this directory and all children.

Potential problems can arise when child directories override parent
configuration settings. In practice, this is almost always intentional. However,
we should elicit an informational log message to notify the user as we enter a
new directory where such a conflict exists.

With such configuration in place, most runner-specific frontmatters become
completely obselete, and equally frontmatters that define fixtures for a given
set of directories.

#### `/tenzir/tests/exec`

These tests sit in their final location.

In case there are comments at the beginning of the file that look like `// key:
value` then we need to translate them into a YAML frontmatter:

```tql
---
key: value
---

// Pipeline continues here.
```

For the `exec` tests, we can remove any mention of `test: exec` because that's
always the default. In many cases, this the only frontmatter directive, so we
can ditch the frontmatter altogether.

If there are other frontmatter configurations, point them out.

#### `/tenzir/tests/ast`

These tests require migration of the corresponding runner.

#### `/tenzir/tests/finalize`

These tests require migration of the corresponding runner.

#### `/tenzir/tests/format_strings`

These tests should actually go as sub directory into /test/language/types/. The
idea is also to move several tests from the `exec` family there, with one
directory per type:

- null
- bool
- ...
- string
- ip
- list
- record

#### `/tenzir/tests/finalize`

These tests require migration of the corresponding runner.

#### `/tenzir/tests/instantiation`

These tests require migration of the corresponding runner.

#### `/tenzir/tests/ir`

These tests require migration of the corresponding runner.

#### `/tenzir/tests/issues`

We can move these over as is.

#### `/tenzir/tests/lambda`

These tests should go under `/test/tests/language/`.

#### `/tenzir/tests/lexer`

These tests require migration of the corresponding runner.

#### `/tenzir/tests/node`

These tests require a different style of migration. In the past, there used to
be a dedicated `node` runner. This thing no longer exists. `tenzir-test` ships
with a `node` fixture that should be used, i.e., the YAML frontmatter should
include `fixtures: [node]` for these tests.

#### `/tenzir/tests/ocsf`

These tests require migration of the corresponding runner.

#### `/tenzir/tests/oldir`

These tests require migration of the corresponding runner.

We should keep a note to consider removal, since these are "old".

#### `/tenzir/tests/opt`

These tests require migration of the corresponding runner.

### Phase 2: Porting of bats test

The old integration tests rely on the bats testing framework.

This requires careful analysis of fixture needs, e.g., tcp.bats is quite tricky.
That said, in many cases these fixtures are _a lot_ simpler to implement with
the new Python fixture framework.

Many tests can also be entirely removed, because they reflect a very old state
of Tenzir where we still had TQL1, the previous language version. For example,
vast.bats is fully obsolete by now. We have to do an analysis what tests are
worth porting and what tests we ditch.

We really want to remove all remnants, including submodules.

### Phase 3: Cleanup

Finally, we'll consider the left-overs.

The list of subheadings here is incomplete, so collect things as we go.

#### `/tenzir/tests/process_coverage.sh`

It's unclear how this script works and whether it provides value. Analyze its
use (CI?) and suggest how to best incorporate it at the framework-level.
