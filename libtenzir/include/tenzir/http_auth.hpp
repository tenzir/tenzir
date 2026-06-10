//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/task.hpp"
#include "tenzir/http.hpp"
#include "tenzir/result.hpp"

#include <string_view>
#include <vector>

namespace tenzir {

class OpCtx;

struct AuthorizationConfig {
  std::vector<http::Header> headers;
};

/// Fetches a named authorization configuration from the runtime.
auto fetch_authorization(std::string_view name, OpCtx& ctx)
  -> Task<failure_or<AuthorizationConfig>>;

} // namespace tenzir
