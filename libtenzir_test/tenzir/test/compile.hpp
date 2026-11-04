//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/tql2/registry.hpp"
#include "tenzir/tql2/resolve.hpp"

#include <string>
#include <string_view>
#include <vector>

namespace tenzir::test {

/// The messages of the errors that compiling `text` against `reg` reports,
/// letting it name the packages in `prelude`.
auto compile_errors(registry const& reg, ExternPrelude const& prelude,
                    std::string_view text) -> std::vector<std::string>;

} // namespace tenzir::test
