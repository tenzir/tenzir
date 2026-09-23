#!/usr/bin/env python
# fixtures: [http, package_http]

import json
import os
from pathlib import Path

node = acquire_fixture("node")
node.start()
tenzir = Executor.from_env(node.env)
base = os.environ["HTTP_FIXTURE_URL"].rstrip("/")
local_root = Path(os.environ["PACKAGE_LOCAL_DIR"])


def run(pipeline):
    result = tenzir.run(pipeline)
    assert result.returncode == 0, result.stderr.decode()
    return result.stdout.decode()


try:
    for path in ("package.yaml?download=true", "redirect"):
        run(f'package_add "{base}/package/{path}", inputs={{message: "custom"}}')
        packages = [
            json.loads(line) for line in run("package_list | write_ndjson").splitlines()
        ]
        package = next(p for p in packages if p["id"] == "remote_test")
        assert package["name"] == "Remote test", package
        assert package["config"]["inputs"]["message"] == "custom", package
        run('package_remove "remote_test"')
    print("ok: HTTP package URLs, query strings, redirects, and inputs")
    for path in (
        "missing.yaml",
        "invalid.yaml",
        "not-record.yaml",
        "invalid-package.yaml",
    ):
        result = tenzir.run(f'package_add "{base}/package/{path}"')
        assert result.returncode != 0, path
        assert "while loading package source" in result.stderr.decode(), (
            result.stderr.decode()
        )
        assert run("package_list | write_ndjson").strip() == ""
    print("ok: HTTP errors and invalid package definitions are rejected")
    archive_base = os.environ["PACKAGE_HTTP_URL"]
    for path in (
        "/archive.tar.gz?sha=main&path=packages/example",
        "/archive.tar",
        "/single-root.tar.gz",
        "/redirect",
        "/api/v4/projects/group%2Frepo/repository/archive?sha=main&path=packages/example",
    ):
        run(f'package_add "{archive_base}{path}"')
        packages = [
            json.loads(line) for line in run("package_list | write_ndjson").splitlines()
        ]
        package = next(p for p in packages if p["id"] == "remote_split")
        assert package["config"]["inputs"]["message"] == "configured", package
        pipelines = [
            json.loads(line)
            for line in run("pipeline_list | write_ndjson").splitlines()
        ]
        pipeline = next(p for p in pipelines if p["id"] == "remote_split/nested/hello")
        assert '"configured"' in pipeline["definition"], pipeline
        extended = json.loads(run('package_list format="extended" | write_ndjson'))
        definition = extended["package_definition"]
        assert "nested::answer" in definition["operators"], definition
        assert len(definition["examples"]) == 1, definition
        assert definition["lets"] == "let $answer = 42\n", definition
        run('package_remove "remote_split"')
    print("ok: tar and gzip archives, GitLab layout, redirects, and split packages")
    run(f'package_add "{archive_base}/derived.tgz", inputs={{message: "override"}}')
    package = json.loads(run("package_list | write_ndjson"))
    assert package["id"] == "derived_package", package
    assert package["config"]["inputs"]["message"] == "configured", package
    # Existing package_add semantics retain values supplied in config.yaml.
    run('package_remove "derived_package"')
    print("ok: archive directory IDs and existing input precedence")
    for archive, path, expected_id in (
        ("multiple.tar.gz", "packages/example", "remote_split"),
        ("multiple-wrapped.tar.gz", "packages/example/", "remote_split"),
        ("multiple-wrapped.tar.gz", "./packages/other", "other"),
        ("single-root.tar.gz", ".", "remote_split"),
        ("derived.tgz", ".", "derived_package"),
        (
            "archive.tar.gz?sha=main&path=packages/example",
            "packages/example",
            "remote_split",
        ),
    ):
        run(f'package_add "{archive_base}/{archive}", path={json.dumps(path)}')
        package = json.loads(run("package_list | write_ndjson"))
        assert package["id"] == expected_id, package
        run(f'package_remove "{expected_id}"')
    print("ok: path selects packages with or without an archive wrapper")
    for path, expected in (
        ("packages/missing", "does not contain package.yaml"),
        ("packages", "does not contain package.yaml"),
        ("../packages/example", "relative archive directory"),
        ("/packages/example", "relative archive directory"),
        ("packages\\example", "relative archive directory"),
        ("", "relative archive directory"),
    ):
        result = tenzir.run(
            f'package_add "{archive_base}/multiple-wrapped.tar.gz", path={json.dumps(path)}'
        )
        assert result.returncode != 0, path
        assert expected in result.stderr.decode(), result.stderr.decode()
    for pipeline, expected in (
        (
            f'package_add "{base}/package/package.yaml", path="."',
            "requires a package archive",
        ),
        (
            f'package_add {json.dumps(str(local_root / "directory"))}, path="."',
            "requires a package archive, not a directory",
        ),
        ('from {} | package_add path="."', "requires a package archive source"),
    ):
        result = tenzir.run(pipeline)
        assert result.returncode != 0, pipeline
        assert expected in result.stderr.decode(), result.stderr.decode()
    assert run("package_list | write_ndjson").strip() == ""
    print("ok: invalid paths and path on non-archive sources are rejected")
    for path, expected in (
        ("traversal", "unsafe package archive path"),
        ("absolute", "unsafe package archive path"),
        ("symlink", "regular files and directories"),
        ("hardlink", "regular files and directories"),
        ("fifo", "regular files and directories"),
        ("oversized", "exceeds 64 MiB"),
        ("duplicate", "duplicate package archive file"),
        ("missing", "exactly one package.yaml"),
        ("ambiguous", "exactly one package.yaml"),
        ("multiple", "exactly one package.yaml"),
        ("multiple-wrapped", "exactly one package.yaml"),
        ("corrupt", "failed to read package archive"),
        ("truncated", "failed to read package archive"),
    ):
        result = tenzir.run(f'package_add "{archive_base}/{path}.tar.gz"')
        assert result.returncode != 0, path
        assert expected in result.stderr.decode(), result.stderr.decode()
        assert run("package_list | write_ndjson").strip() == ""
    print("ok: unsafe, oversized, ambiguous, and invalid archives are rejected")
    for archive, path, expected_id in (
        ("archive.tar", None, "remote_split"),
        ("archive.tar.gz", None, "remote_split"),
        ("single-root.tar.gz", None, "remote_split"),
        ("derived.tgz", None, "derived_package"),
        ("archive", None, "remote_split"),
        ("multiple.tar.gz", "packages/example", "remote_split"),
        ("multiple-wrapped.tar.gz", "packages/other", "other"),
    ):
        pipeline = f"package_add {json.dumps(str(local_root / archive))}"
        if path is not None:
            pipeline += f", path={json.dumps(path)}"
        run(pipeline)
        package = json.loads(run("package_list | write_ndjson"))
        assert package["id"] == expected_id, package
        run(f'package_remove "{expected_id}"')
    print("ok: local tar, gzip, and tgz archives share discovery and path selection")
    for name, expected in (
        ("traversal.tar.gz", "unsafe package archive path"),
        ("symlink.tar.gz", "regular files and directories"),
        ("multiple.tar.gz", "exactly one package.yaml"),
        ("corrupt.tar.gz", "failed to read package archive"),
        ("absent.tar.gz", "package source"),
    ):
        result = tenzir.run(f"package_add {json.dumps(str(local_root / name))}")
        assert result.returncode != 0, name
        assert expected in result.stderr.decode(), result.stderr.decode()
    print("ok: invalid local archives are rejected")
    for source in (
        str(local_root / "directory"),
        str(local_root / "warning.tar.gz"),
        f"{archive_base}/warning.tar.gz",
    ):
        result = tenzir.run(f"package_add {json.dumps(source)}")
        assert result.returncode == 0, result.stderr.decode()
        assert (
            "warning: package id `warning-package` contains '-' characters"
            in result.stderr.decode()
        ), result.stderr.decode()
        assert "error:" not in result.stderr.decode(), result.stderr.decode()
        assert json.loads(run("package_list | write_ndjson"))["id"] == "warning-package"
        run('package_remove "warning-package"')
    print("ok: directory, local archive, and remote archive warnings remain warnings")
    for source in (
        str(local_root / "warning-error.tar.gz"),
        f"{archive_base}/warning-error.tar.gz",
    ):
        result = tenzir.run(f"package_add {json.dumps(source)}")
        assert result.returncode != 0, source
        assert "warning:" in result.stderr.decode(), result.stderr.decode()
        assert "error:" in result.stderr.decode(), result.stderr.decode()
        assert run("package_list | write_ndjson").strip() == ""
    print("ok: warnings accompanying package errors are preserved without installation")
finally:
    node.stop()
