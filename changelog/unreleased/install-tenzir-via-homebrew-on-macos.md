---
title: Install Tenzir via Homebrew on macOS
type: feature
author: mavam
pr: 5876
created: 2026-03-08T06:02:24.0267Z
---

You can now install Tenzir on Apple Silicon macOS via Homebrew:

```sh
brew tap tenzir/tenzir
brew install --cask tenzir
```

You can also install directly without tapping first:

```sh
brew install --cask tenzir/tenzir/tenzir
```

The release workflow keeps the Homebrew cask in sync with the signed macOS
package so installs and uninstalls stay current across releases.
