# External Files

When integrating third-party code into the Tenzir codebase, use this scaffold:

```cpp
//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
//
// This file comes from a 3rd party and has been adapted to fit into the Tenzir
// code base. Details about the original file:
//
// - Repository: https://github.com/example/lib
// - Commit:     abc123def456...
// - Path:       src/feature.hpp
// - Author:     Original Author
// - Copyright:  (c) 2020 Original Author
// - License:    MIT

// ... adapted code follows ...
```

## Required Fields

- **Repository**: Full URL to the source repository
- **Commit**: Full commit hash for reproducibility
- **Path**: Original file path in the source repository
- **Author**: Original author name
- **Copyright**: Original copyright notice
- **License**: Original license (must be compatible with BSD-3-Clause)

## Guidelines

- Preserve the original license compatibility
- Document all modifications in code comments
- Keep adaptations minimal—prefer wrapping over modifying
