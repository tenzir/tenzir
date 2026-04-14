#!/usr/bin/env python3
"""Render the managed Homebrew cask contents for Tenzir."""

import argparse
from pathlib import Path


def render_cask(version: str, sha256: str, pkgutil_identifier: str) -> str:
    return f'''cask "tenzir" do
  version "{version}"
  sha256 "{sha256}"

  arch arm: "arm64"

  url "https://github.com/tenzir/tenzir/releases/download/v#{{version}}/tenzir-#{{version}}-#{{arch}}-darwin-static.pkg"
  name "Tenzir"
  desc "Data pipelines for security teams"
  homepage "https://github.com/tenzir/tenzir"

  livecheck do
    skip "Managed by tenzir/tenzir release automation."
  end

  depends_on arch: :arm64

  pkg "tenzir-#{{version}}-#{{arch}}-darwin-static.pkg"

  uninstall pkgutil: "{pkgutil_identifier}"
end
'''


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cask", required=True, help="Path to Casks/tenzir.rb")
    parser.add_argument(
        "--version", required=True, help="Release version without the leading v"
    )
    parser.add_argument("--sha256", required=True, help="SHA256 of the release package")
    parser.add_argument(
        "--pkgutil-identifier",
        required=True,
        help="pkgutil identifier used for brew uninstall",
    )
    args = parser.parse_args()

    cask_path = Path(args.cask)
    cask_path.parent.mkdir(parents=True, exist_ok=True)
    cask_path.write_text(
        render_cask(args.version, args.sha256, args.pkgutil_identifier),
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
