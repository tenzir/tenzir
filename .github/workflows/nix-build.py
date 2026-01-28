#!/usr/bin/env python3
"""
Build and publish Tenzir static packages and container images using Nix.

This script provides subcommands for each phase of the build/publish pipeline:

    build   - Build the Nix package
    test    - Test installation in containers (Linux only)
    sign    - Sign macOS packages (macOS only)
    upload  - Upload packages to stores and GitHub releases
    push    - Push container images to registries

Examples:
    # Build only (local development)
    ./nix-build.py build --attribute tenzir-static

    # Full pipeline for CI
    ./nix-build.py build --attribute tenzir-static
    ./nix-build.py package-test --package-dir ./packages --arch x86_64
    ./nix-build.py sign --package-dir ./packages
    ./nix-build.py upload --package-dir ./packages --package-store gcs:bucket/path
    ./nix-build.py push --attribute tenzir-static --arch x86_64 --container-tag main --image-registry ghcr.io
"""

import argparse
import base64
import gzip
import os
import platform
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time

from pathlib import Path


def notice(msg: str) -> None:
    print(f"::notice::{msg}")


def warning(msg: str) -> None:
    print(f"::warning::{msg}")


def error(msg: str) -> None:
    print(f"::error::{msg}")


def run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    """Run a command, optionally capturing output."""
    notice(f"Running: {' '.join(cmd)}")
    return subprocess.run(cmd, check=check, stdout=subprocess.PIPE, text=True)


def set_output(name: str, value: str) -> None:
    """Set a GitHub Actions output variable."""
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"{name}={value}\n")
    notice(f"Output {name}={value}")


def registry_login(registry: str) -> bool:
    """Login to container registries using skopeo.

    Credentials are read from environment variables:
      - GHCR_TOKEN: GitHub token for ghcr.io
      - DOCKERHUB_USER, DOCKERHUB_PASSWORD: Docker Hub credentials
      - AWS credentials (via environment or ~/.aws) for ECR registries
    """
    if registry == "ghcr.io":
        token = os.environ.get("GHCR_TOKEN", "")
        if not token:
            warning("GHCR_TOKEN not set, skipping ghcr.io login")
            return False
        notice("Logging in to ghcr.io")
        _ = run(["skopeo", "login", "ghcr.io", "-u", "tenzir-bot", "-p", token])
    elif registry == "docker.io":
        user = os.environ.get("DOCKERHUB_USER", "")
        password = os.environ.get("DOCKERHUB_PASSWORD", "")
        if not user or not password:
            warning("DOCKERHUB credentials not set, skipping docker.io login")
            return False
        notice("Logging in to docker.io")
        _ = run(["skopeo", "login", "docker.io", "-u", user, "-p", password])
    elif ".dkr.ecr." in registry and ".amazonaws.com" in registry:
        # ECR registry - use boto3 to get credentials
        # Assumes AWS credentials are configured via environment or ~/.aws
        import boto3

        notice(f"Logging in to ECR registry {registry}")
        # Extract region from registry URL (e.g., 622024652768.dkr.ecr.eu-west-1.amazonaws.com)
        parts = registry.split(".")
        try:
            region_idx = parts.index("ecr") + 1
            region = parts[region_idx]
        except (ValueError, IndexError):
            error(f"Could not extract region from ECR registry URL: {registry}")
            return False
        # Get ECR authorization token
        try:
            ecr = boto3.client("ecr", region_name=region)
            response = ecr.get_authorization_token()
            auth_data = response["authorizationData"][0]
            # Token is base64 encoded "AWS:<password>"
            token = base64.b64decode(auth_data["authorizationToken"]).decode()
            _, password = token.split(":", 1)
            _ = run(["skopeo", "login", registry, "-u", "AWS", "-p", password])
        except Exception as e:
            warning(f"Failed to get ECR credentials for {registry}: {e}")
            return False
    else:
        return False
    return True


# =============================================================================
# Subcommand: build
# =============================================================================


def cmd_build(args: argparse.Namespace) -> int:
    """Build the Nix package and output the package directory."""
    # Update the tenzir-plugins submodule source info.
    _ = run(["nix/update-plugins.sh"])

    release_input = (
        "github:boolean-option/true" if args.release else "github:boolean-option/false"
    )
    cmd = [
        "nix",
        "--accept-flake-config",
        "--print-build-logs",
        "build",
        f".#{args.attribute}^package",
        "--override-input",
        "isReleaseBuild",
        release_input,
        "--no-link",
        "--print-out-paths",
    ]
    notice(f"Building {args.attribute}")
    result = run(cmd)
    pkg_dir = Path(result.stdout.strip())

    # Copy to local directory so subsequent steps can modify (Nix store is read-only)
    local_pkg_dir = Path("./packages")
    local_pkg_dir.mkdir(exist_ok=True)
    for pkg_file in pkg_dir.iterdir():
        shutil.copy2(pkg_file, local_pkg_dir / pkg_file.name)
    notice(f"Copied packages to {local_pkg_dir}")

    set_output("package-dir", str(local_pkg_dir.resolve()))
    return 0


# =============================================================================
# Subcommand: test
# =============================================================================


def run_container_test(
    container_name: str,
    image: str,
    pkg_dir: Path,
    pkg_pattern: str,
    arch: str,
    test_root: str,
    test_pattern: str,
) -> bool:
    """Run installation tests in a Docker container.

    Args:
        container_name: Name for the Docker container
        image: Docker image to use (or path to build context)
        pkg_dir: Directory containing packages
        pkg_pattern: Glob pattern for package file (e.g., "tenzir*.deb")
        arch: Architecture (x86_64 or aarch64)
        test_root: Root directory for tenzir-test
        test_pattern: Test pattern to run (e.g., "tests/functions/time")

    Returns:
        True if tests passed, False otherwise.
    """
    # Find the package file
    pkg_files = list(pkg_dir.glob(pkg_pattern))
    if not pkg_files:
        warning(f"No package matching {pkg_pattern} found in {pkg_dir}")
        return False
    pkg_file = pkg_files[0].name

    # Build the test image if it's a local path, otherwise pull from registry
    if Path(image).is_dir():
        notice(f"Building test image from {image}")
        result = subprocess.run(
            ["docker", "build", image, "-t", container_name],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            error(f"Failed to build test image: {result.stderr}")
            return False
        image = container_name

    # Start the container
    notice(f"Starting container {container_name}")
    result = subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--name",
            container_name,
            "--privileged",
            "--volume",
            ".:/src/tenzir:ro",
            "--volume",
            f"{pkg_dir.resolve()}:/tmp/packages:ro",
            "--volume",
            "/sys/fs/cgroup:/sys/fs/cgroup:rw",
            "--cgroupns=host",
            image,
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        error(f"Failed to start container: {result.stderr}")
        return False

    time.sleep(5)  # Wait for systemd to initialize

    try:
        # Install tenzir from package
        notice(f"Installing tenzir from {pkg_file}")
        result = subprocess.run(
            [
                "docker",
                "exec",
                "-e",
                f"TENZIR_PACKAGE_URL=file:///tmp/packages/{pkg_file}",
                container_name,
                "bash",
                "-c",
                "/src/tenzir/scripts/install.sh",
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            error(f"Installation failed: {result.stderr}")
            return False

        # Create a test pipeline
        notice("Creating test pipeline")
        pipeline_args = '{"definition": "from \\"udp://0.0.0.0:514\\" { read_syslog } | discard", "autostart": {"created": true}}'
        result = subprocess.run(
            [
                "docker",
                "exec",
                "-e",
                "TENZIR_LEGACY=true",
                container_name,
                "/opt/tenzir/bin/tenzir",
                f"api /pipeline/create '{pipeline_args}'",
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            error(f"Pipeline creation failed: {result.stderr}")
            return False

        # Install dependencies for tenzir-test
        notice("Installing test dependencies")
        if "ubuntu" in container_name.lower():
            result = subprocess.run(
                [
                    "docker",
                    "exec",
                    container_name,
                    "bash",
                    "-c",
                    "apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y coreutils python3 tzdata",
                ],
                capture_output=True,
                text=True,
            )
        else:
            # Rocky Linux - fewer deps needed
            result = subprocess.CompletedProcess(args=[], returncode=0)

        if result.returncode != 0:
            error(f"Dependency installation failed: {result.stderr}")
            return False

        # Install uv
        notice("Installing uv")
        result = subprocess.run(
            [
                "docker",
                "exec",
                container_name,
                "bash",
                "-c",
                "curl -LsSf https://astral.sh/uv/install.sh | sh",
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            error(f"uv installation failed: {result.stderr}")
            return False

        # Run tenzir-test
        notice(f"Running tenzir-test: {test_pattern}")
        env_args = ["-e", "TENZIR_ALLOC_STATS=1"]
        if arch == "x86_64":
            env_args += ["-e", "TENZIR_ALLOC_ACTOR_STATS=1"]

        test_cmd = f'export PATH="/opt/tenzir/bin:/root/.local/bin:/usr/bin:/bin:$PATH" && uvx tenzir-test --root /src/tenzir/{test_root} {test_pattern}'
        result = subprocess.run(
            ["docker", "exec"] + env_args + [container_name, "bash", "-c", test_cmd],
            capture_output=True,
            text=True,
        )
        print(result.stdout)  # Show test output
        if result.returncode != 0:
            error(f"tenzir-test failed: {result.stderr}")
            return False

        notice(f"Tests passed for {container_name}")
        return True

    finally:
        # Stop and remove container
        _ = subprocess.run(
            ["docker", "exec", container_name, "halt"],
            capture_output=True,
        )
        _ = subprocess.run(
            ["docker", "rm", "-f", container_name],
            capture_output=True,
        )


def cmd_test(args: argparse.Namespace) -> int:
    """Run installation tests in containers (Linux only)."""
    if platform.system() != "Linux":
        notice("Skipping container tests on non-Linux platform")
        return 0

    pkg_dir = Path(args.package_dir)
    test_root = args.test_root
    test_pattern = args.test_pattern

    success = True

    # Test on Ubuntu
    if not run_container_test(
        container_name="installer-test-ubuntu",
        image="tenzir/services/systemd/test",
        pkg_dir=pkg_dir,
        pkg_pattern="tenzir*.deb",
        arch=args.arch,
        test_root=test_root,
        test_pattern=test_pattern,
    ):
        success = False

    # Test on Rocky Linux
    if not run_container_test(
        container_name="installer-test-rocky",
        image="eniocarboni/docker-rockylinux-systemd:9",
        pkg_dir=pkg_dir,
        pkg_pattern="tenzir*.rpm",
        arch=args.arch,
        test_root=test_root,
        test_pattern=test_pattern,
    ):
        success = False

    return 0 if success else 1


# =============================================================================
# Subcommand: sign
# =============================================================================


def cmd_sign(args: argparse.Namespace) -> int:
    """Sign macOS packages (.tar.gz and .pkg)."""
    if platform.system() != "Darwin":
        notice("Skipping macOS signing on non-Darwin platform")
        set_output("package-dir", str(args.package_dir))
        return 0

    pkg_dir = Path(args.package_dir)
    signing_identity = args.signing_identity or os.environ.get("APPLE_SIGNING_IDENTITY")
    installer_identity = args.installer_identity or os.environ.get(
        "APPLE_INSTALLER_IDENTITY"
    )

    if not signing_identity:
        warning("No signing identity provided, skipping code signing")
        set_output("package-dir", str(pkg_dir.resolve()))
        return 0

    tarballs = list(pkg_dir.glob("*.tar.gz"))
    pkgs = list(pkg_dir.glob("*.pkg"))

    if not tarballs:
        warning("No tarball found for signing")
        set_output("package-dir", str(pkg_dir.resolve()))
        return 0

    if len(tarballs) != 1:
        warning(f"Expected exactly one tarball, found {len(tarballs)}")
        set_output("package-dir", str(pkg_dir.resolve()))
        return 0

    # Create output directory for signed packages
    signed_pkg_dir = Path("./signed-packages")
    signed_pkg_dir.mkdir(exist_ok=True)

    tarball = tarballs[0]
    notice(f"Signing binary in {tarball.name}")

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        extract_dir = tmp_path / "extracted"
        extract_dir.mkdir()

        # Extract tarball
        with tarfile.open(tarball, "r:gz") as tf:
            tf.extractall(extract_dir, filter="data")

        # Find and sign the binary
        binary_path = extract_dir / "opt" / "tenzir" / "bin" / "tenzir"
        if not binary_path.exists():
            error(f"Binary not found at {binary_path}")
            return 1

        notice(f"Signing {binary_path}")
        result = subprocess.run(
            [
                "codesign",
                "--force",
                "--sign",
                signing_identity,
                "--timestamp",
                "--options",
                "runtime",
                str(binary_path),
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            error(f"codesign failed: {result.stderr}")
            return 1

        # Verify signature
        result = subprocess.run(
            ["codesign", "--verify", "--verbose", str(binary_path)],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            error(f"Signature verification failed: {result.stderr}")
            return 1
        notice("Signature verified successfully")

        # Repack tarball to signed-packages directory
        signed_tarball = signed_pkg_dir / tarball.name
        with tarfile.open(signed_tarball, "w:gz") as tf:
            # Add contents with the correct archive name (opt/tenzir/...)
            for item in extract_dir.iterdir():
                tf.add(item, arcname=item.name)
        notice(f"Created signed tarball: {signed_tarball}")

        # Now handle the .pkg if present
        if pkgs:
            if len(pkgs) != 1:
                warning(f"Expected exactly one .pkg, found {len(pkgs)}")
            else:
                pkg = pkgs[0]
                notice(f"Updating binary in {pkg.name}")

                # pkgutil --expand creates the destination directory itself
                pkg_extract_dir = tmp_path / "pkg_extracted"

                # Expand the .pkg (it's a xar archive with component packages)
                result = subprocess.run(
                    ["pkgutil", "--expand", str(pkg), str(pkg_extract_dir)],
                    capture_output=True,
                    text=True,
                )
                if result.returncode != 0:
                    error(f"pkgutil --expand failed: {result.stderr}")
                    return 1

                # Find the Payload in the component package
                # The structure is: pkg_extracted/<component>.pkg/Payload
                component_dirs = [
                    d
                    for d in pkg_extract_dir.iterdir()
                    if d.is_dir() and d.suffix == ".pkg"
                ]
                if not component_dirs:
                    error("No component package found in .pkg")
                    return 1

                component_dir = component_dirs[0]
                payload_path = component_dir / "Payload"

                if not payload_path.exists():
                    error(f"Payload not found at {payload_path}")
                    return 1

                # Extract Payload (gzipped cpio archive)
                payload_extract_dir = tmp_path / "payload_extracted"
                payload_extract_dir.mkdir()

                # Use gzip to decompress and cpio to extract
                with gzip.open(payload_path, "rb") as gz:
                    result = subprocess.run(
                        ["cpio", "-id", "--quiet"],
                        input=gz.read(),
                        cwd=payload_extract_dir,
                        capture_output=True,
                    )
                    if result.returncode != 0:
                        error(f"cpio extraction failed: {result.stderr.decode()}")
                        return 1

                # Replace the binary with the signed one
                pkg_binary_path = (
                    payload_extract_dir / "opt" / "tenzir" / "bin" / "tenzir"
                )
                if pkg_binary_path.exists():
                    shutil.copy2(binary_path, pkg_binary_path)
                    notice("Replaced binary in .pkg payload with signed version")
                else:
                    warning(
                        f"Binary not found in .pkg at expected path: {pkg_binary_path}"
                    )

                # Recreate Payload (cpio | gzip)
                # Get list of files relative to payload_extract_dir
                result = subprocess.run(
                    ["find", ".", "-print"],
                    cwd=payload_extract_dir,
                    capture_output=True,
                    text=True,
                )
                file_list = result.stdout

                # Create cpio archive
                cpio_result = subprocess.run(
                    ["cpio", "-o", "--format=odc", "--quiet"],
                    input=file_list.encode(),
                    cwd=payload_extract_dir,
                    capture_output=True,
                )
                if cpio_result.returncode != 0:
                    error(f"cpio creation failed: {cpio_result.stderr.decode()}")
                    return 1

                # Gzip and write back
                with gzip.open(payload_path, "wb") as gz:
                    gz.write(cpio_result.stdout)

                # Flatten the package to a temporary location first
                unsigned_pkg = tmp_path / f"unsigned-{pkg.name}"
                result = subprocess.run(
                    ["pkgutil", "--flatten", str(pkg_extract_dir), str(unsigned_pkg)],
                    capture_output=True,
                    text=True,
                )
                if result.returncode != 0:
                    error(f"pkgutil --flatten failed: {result.stderr}")
                    return 1

                # Sign the .pkg with productsign if installer identity is available
                signed_pkg = signed_pkg_dir / pkg.name
                if installer_identity:
                    notice(
                        f"Signing .pkg with installer identity: {installer_identity}"
                    )
                    result = subprocess.run(
                        [
                            "productsign",
                            "--sign",
                            installer_identity,
                            "--timestamp",
                            str(unsigned_pkg),
                            str(signed_pkg),
                        ],
                        capture_output=True,
                        text=True,
                    )
                    if result.returncode != 0:
                        error(f"productsign failed: {result.stderr}")
                        return 1

                    # Verify the .pkg signature
                    result = subprocess.run(
                        ["pkgutil", "--check-signature", str(signed_pkg)],
                        capture_output=True,
                        text=True,
                    )
                    if result.returncode != 0:
                        error(f".pkg signature verification failed: {result.stderr}")
                        return 1
                    notice(f"Created and signed .pkg: {signed_pkg}")
                else:
                    # No installer identity, just copy the unsigned pkg
                    shutil.copy2(unsigned_pkg, signed_pkg)
                    warning("No installer identity provided, .pkg is unsigned")
                    notice(f"Created .pkg (unsigned): {signed_pkg}")

    set_output("package-dir", str(signed_pkg_dir.resolve()))
    return 0


# =============================================================================
# Subcommand: upload
# =============================================================================


def cmd_upload(args: argparse.Namespace) -> int:
    """Upload packages to stores and optionally attach to GitHub release."""
    pkg_dir = Path(args.package_dir)

    if not args.package_stores and not args.release_tag:
        notice("No package stores or release tag specified, skipping upload")
        return 0

    # Collect all package files
    packages = {
        "rpm": list(pkg_dir.glob("*.rpm")),
        "debian": list(pkg_dir.glob("*.deb")),
        "macOS": list(pkg_dir.glob("*.pkg")),
        "tarball": list(pkg_dir.glob("*.tar.gz")),
    }

    # Upload to remote stores
    for store in args.package_stores:
        store_type = store.split(":")[0]
        env = os.environ.copy()
        env[f"RCLONE_CONFIG_{store_type.upper()}_TYPE"] = store_type

        for label, files in packages.items():
            for pkg_file in files:
                dest = f"{store}/{label}/{pkg_file.name}"
                notice(f"Copying artifact to {dest}")
                _ = subprocess.run(
                    ["rclone", "-q", "copyto", str(pkg_file), dest], env=env, check=True
                )

                # Create alias copies
                for alias in args.package_aliases:
                    alias_name = re.sub(r"[0-9]+\.[0-9]+\.[0-9]+", alias, pkg_file.name)
                    alias_dest = f"{store}/{label}/{alias_name}"
                    notice(f"Copying artifact to {alias_dest}")
                    _ = subprocess.run(
                        ["rclone", "-q", "copyto", dest, alias_dest],
                        env=env,
                        check=True,
                    )

    # Attach to GitHub release
    if args.release_tag:
        for pkg_file in pkg_dir.iterdir():
            if pkg_file.is_file():
                notice(f"Attaching {pkg_file.name} to {args.release_tag}")
                _ = run(
                    [
                        "gh",
                        "release",
                        "upload",
                        args.release_tag,
                        str(pkg_file),
                        "--clobber",
                    ]
                )

    return 0


# =============================================================================
# Subcommand: push
# =============================================================================


def cmd_push(args: argparse.Namespace) -> int:
    """Push container images to registries."""
    if platform.system() != "Linux":
        notice("Skipping image push on non-Linux platform")
        return 0

    if not args.image_registries or not args.container_tags:
        notice("No registries or tags specified, skipping image push")
        return 0

    release_input = (
        "github:boolean-option/true" if args.release else "github:boolean-option/false"
    )
    tag_suffix = "-slim" if "-static" in args.attribute else ""
    arch_suffix = f"-{args.arch}" if args.arch else ""

    pushed_images: list[str] = []

    # We always push two images: tenzir and tenzir-node
    for registry in args.image_registries:
        if not registry_login(registry):
            continue
        for repo in ["tenzir", "tenzir-node"]:
            for tag in args.container_tags:
                full_tag = f"{tag}{tag_suffix}{arch_suffix}"
                dest = f"docker://{registry}/tenzir/{repo}:{full_tag}"
                notice(f"Pushing {dest}")
                _ = run(
                    [
                        "nix",
                        "--accept-flake-config",
                        "run",
                        f".#{args.attribute}.asImage.{repo}.copyTo",
                        "--override-input",
                        "isReleaseBuild",
                        release_input,
                        "--",
                        dest,
                    ]
                )
                pushed_images.append(f"{registry}/tenzir/{repo}:{full_tag}")

    if pushed_images:
        set_output("images", " ".join(pushed_images))

    return 0


# =============================================================================
# Main
# =============================================================================


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- build subcommand ---
    build_parser = subparsers.add_parser("build", help="Build the Nix package")
    _ = build_parser.add_argument(
        "--attribute",
        required=True,
        help="Nix attribute to build (e.g., tenzir-static)",
    )
    _ = build_parser.add_argument(
        "--release",
        action="store_true",
        help="Build in release mode",
    )
    build_parser.set_defaults(func=cmd_build)

    # --- package-test subcommand ---
    test_parser = subparsers.add_parser(
        "package-test", help="Test package installation in containers (Linux only)"
    )
    _ = test_parser.add_argument(
        "--package-dir",
        required=True,
        help="Directory containing packages to test",
    )
    _ = test_parser.add_argument(
        "--arch",
        required=True,
        help="Architecture (x86_64 or aarch64)",
    )
    _ = test_parser.add_argument(
        "--test-root",
        default="test",
        help="Root directory for tenzir-test (default: test)",
    )
    _ = test_parser.add_argument(
        "--test-pattern",
        default="tests/functions/time",
        help="Test pattern to run (default: tests/functions/time)",
    )
    test_parser.set_defaults(func=cmd_test)

    # --- sign subcommand ---
    sign_parser = subparsers.add_parser("sign", help="Sign macOS packages")
    _ = sign_parser.add_argument(
        "--package-dir",
        required=True,
        help="Directory containing packages to sign",
    )
    _ = sign_parser.add_argument(
        "--signing-identity",
        help="Code signing identity (or set APPLE_SIGNING_IDENTITY env var)",
    )
    _ = sign_parser.add_argument(
        "--installer-identity",
        help="Installer signing identity (or set APPLE_INSTALLER_IDENTITY env var)",
    )
    sign_parser.set_defaults(func=cmd_sign)

    # --- upload subcommand ---
    upload_parser = subparsers.add_parser(
        "upload", help="Upload packages to stores and GitHub releases"
    )
    _ = upload_parser.add_argument(
        "--package-dir",
        required=True,
        help="Directory containing packages to upload",
    )
    _ = upload_parser.add_argument(
        "--package-store",
        action="append",
        dest="package_stores",
        default=[],
        help="Package store URL for rclone (can be specified multiple times)",
    )
    _ = upload_parser.add_argument(
        "--package-alias",
        action="append",
        dest="package_aliases",
        default=[],
        help="Alias for packages, replaces version in filename (can be specified multiple times)",
    )
    _ = upload_parser.add_argument(
        "--release-tag",
        help="Git tag for release (attaches packages to GitHub release)",
    )
    upload_parser.set_defaults(func=cmd_upload)

    # --- push subcommand ---
    push_parser = subparsers.add_parser(
        "push", help="Push container images to registries"
    )
    _ = push_parser.add_argument(
        "--attribute",
        required=True,
        help="Nix attribute for image source (e.g., tenzir-static)",
    )
    _ = push_parser.add_argument(
        "--arch",
        required=True,
        help="Architecture suffix for container tags (e.g., aarch64, x86_64)",
    )
    _ = push_parser.add_argument(
        "--container-tag",
        action="append",
        dest="container_tags",
        default=[],
        help="Container image tag (can be specified multiple times)",
    )
    _ = push_parser.add_argument(
        "--image-registry",
        action="append",
        dest="image_registries",
        default=[],
        help="Container registry to push to (can be specified multiple times)",
    )
    _ = push_parser.add_argument(
        "--release",
        action="store_true",
        help="Build images in release mode",
    )
    push_parser.set_defaults(func=cmd_push)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
