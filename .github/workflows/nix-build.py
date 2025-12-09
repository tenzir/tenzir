#!/usr/bin/env python3
"""
Build and publish Tenzir static packages and container images using Nix.

Examples:
    # Local development build (no publishing)
    ./nix-build.py --attribute tenzir-static

    # Build and push to ghcr.io with a specific tag
    ./nix-build.py --attribute tenzir-static --container-tag my-branch --image-registry ghcr.io

    # Release build with package upload
    ./nix-build.py --attribute tenzir-static \\
        --container-tag latest --container-tag v1.2.3 \\
        --image-registry ghcr.io --image-registry docker.io \\
        --package-store gcs:tenzir-dist-public/packages/main \\
        --package-alias latest \\
        --release-tag v1.2.3
"""

import argparse
import base64
import os
import platform
import re
import subprocess
import sys

from pathlib import Path


def notice(msg: str) -> None:
    print(f"::notice {msg}")


def warning(msg: str) -> None:
    print(f"::warning {msg}")


def error(msg: str) -> None:
    print(f"::error {msg}")


def run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    """Run a command, optionally capturing output."""
    notice(f"Running: {' '.join(cmd)}")
    return subprocess.run(cmd, check=check, stdout=subprocess.PIPE, text=True)


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


def build(attribute: str, is_release: bool) -> Path:
    """Build the Nix package and return the package output directory."""
    # Update the tenzir-plugins submodule source info.
    _ = run(["nix/update-plugins.sh"])

    release_input = "github:boolean-option/true" if is_release else "github:boolean-option/false"
    cmd = [
        "nix",
        "--accept-flake-config",
        "--print-build-logs",
        "build",
        f".#{attribute}^package",
        "--override-input", "isReleaseBuild", release_input,
        "--no-link",
        "--print-out-paths",
    ]
    notice(f"Building {attribute}")
    result = run(cmd)
    return Path(result.stdout.strip())


def upload_packages(
    pkg_dir: Path,
    package_stores: list[str],
    aliases: list[str],
    release_tag: str | None,
) -> None:
    """Upload packages to stores and optionally attach to GitHub release."""
    # Collect all package files
    packages = {
        "rpm": list(pkg_dir.glob("*.rpm")),
        "debian": list(pkg_dir.glob("*.deb")),
        "macOS": list(pkg_dir.glob("*.pkg")),
        "tarball": list(pkg_dir.glob("*.tar.gz")),
    }

    # Upload to remote stores
    for store in package_stores:
        store_type = store.split(":")[0]
        env = os.environ.copy()
        env[f"RCLONE_CONFIG_{store_type.upper()}_TYPE"] = store_type

        for label, files in packages.items():
            for pkg_file in files:
                dest = f"{store}/{label}/{pkg_file.name}"
                notice(f"Copying artifact to {dest}")
                _ = subprocess.run(["rclone", "-q", "copyto", str(pkg_file), dest], env=env, check=True)

                # Create alias copies
                for alias in aliases:
                    alias_name = re.sub(r"[0-9]+\.[0-9]+\.[0-9]+", alias, pkg_file.name)
                    alias_dest = f"{store}/{label}/{alias_name}"
                    notice(f"Copying artifact to {alias_dest}")
                    _ = subprocess.run(["rclone", "-q", "copyto", dest, alias_dest], env=env, check=True)

    # Copy to local packages directory for test steps
    notice("Copying artifacts to ./packages/")
    for label, files in packages.items():
        local_dir = Path(f"./packages/{label}")
        local_dir.mkdir(parents=True, exist_ok=True)
        for pkg_file in files:
            _ = subprocess.run(["cp", "-v", str(pkg_file), str(local_dir)], check=True)

    # Attach to GitHub release
    if release_tag:
        for pkg_file in pkg_dir.iterdir():
            notice(f"Attaching {pkg_file} to {release_tag}")
            _ = run(["gh", "release", "upload", release_tag, str(pkg_file), "--clobber"])


def push_images(
    attribute: str,
    image_registries: list[str],
    container_tags: list[str],
    arch: str | None,
    is_release: bool,
) -> list[str]:
    """Push container images to registries. Returns list of pushed image URLs."""
    pushed_images: list[str] = []

    if platform.system() != "Linux":
        notice("Skipping image push on non-Linux platform")
        return pushed_images

    if not image_registries or not container_tags:
        notice("No registries or tags specified, skipping image push")
        return pushed_images

    release_input = "github:boolean-option/true" if is_release else "github:boolean-option/false"
    tag_suffix = "-slim" if "-static" in attribute else ""
    arch_suffix = f"-{arch}" if arch else ""

    # We always push two images: tenzir and tenzir-node
    for registry in image_registries:
        if not registry_login(registry):
            continue
        for repo in ["tenzir", "tenzir-node"]:
            for tag in container_tags:
                full_tag = f"{tag}{tag_suffix}{arch_suffix}"
                dest = f"docker://{registry}/tenzir/{repo}:{full_tag}"
                notice(f"Pushing {dest}")
                _ = run([
                    "nix",
                    "--accept-flake-config",
                    "run",
                    f".#{attribute}.asImage.{repo}.copyTo",
                    "--override-input", "isReleaseBuild", release_input,
                    "--",
                    dest,
                ])
                pushed_images.append(f"{registry}/tenzir/{repo}:{full_tag}")

    return pushed_images


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _ = parser.add_argument(
        "--attribute",
        required=True,
        help="Nix attribute to build (e.g., tenzir-static)",
    )
    _ = parser.add_argument(
        "--container-tag",
        action="append",
        dest="container_tags",
        default=[],
        help="Container image tag (can be specified multiple times)",
    )
    _ = parser.add_argument(
        "--image-registry",
        action="append",
        dest="image_registries",
        default=[],
        help="Container registry to push to (can be specified multiple times)",
    )
    _ = parser.add_argument(
        "--package-store",
        action="append",
        dest="package_stores",
        default=[],
        help="Package store URL for rclone (can be specified multiple times)",
    )
    _ = parser.add_argument(
        "--package-alias",
        action="append",
        dest="package_aliases",
        default=[],
        help="Alias for packages, replaces version in filename (can be specified multiple times)",
    )
    _ = parser.add_argument(
        "--release-tag",
        default=None,
        help="Git tag for release (enables release mode and GitHub release upload)",
    )
    _ = parser.add_argument(
        "--arch",
        required=True,
        help="Architecture suffix for container tags (e.g., aarch64, x86_64)",
    )

    args = parser.parse_args()

    is_release = args.release_tag is not None

    # Build
    pkg_dir = build(args.attribute, is_release)
    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a") as github_output:
            _ = github_output.write(f"package-dir={pkg_dir}\n")


    # Upload packages
    if args.package_stores or args.release_tag:
        upload_packages(
            pkg_dir,
            args.package_stores,
            args.package_aliases,
            args.release_tag,
        )

    # Push images
    pushed_images: list[str] = []
    if args.image_registries and args.container_tags:
        pushed_images = push_images(
            args.attribute,
            args.image_registries,
            args.container_tags,
            args.arch,
            is_release,
        )

    # Output pushed images for GitHub Actions
    if pushed_images:
        images_str = " ".join(pushed_images)
        notice(f"Pushed images: {images_str}")
        github_output = os.environ.get("GITHUB_OUTPUT")
        if github_output:
            with open(github_output, "a") as f:
                f.write(f"images={images_str}\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
