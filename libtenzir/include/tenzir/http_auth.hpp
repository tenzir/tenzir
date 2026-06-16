//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/aliases.hpp"
#include "tenzir/async/task.hpp"
#include "tenzir/http.hpp"
#include "tenzir/result.hpp"

#include <string_view>
#include <vector>

namespace tenzir {

class OpCtx;
struct platform_authentication;

struct AuthorizationConfig {
  std::vector<http::Header> headers;
};

/// Fetches a named authorization configuration from the runtime.
auto fetch_authorization(std::string_view name, OpCtx& ctx)
  -> Task<failure_or<AuthorizationConfig>>;

/// Materializes a platform-resolved authentication into the flat `record` shape
/// the operator-side strategy parsers consume. Public-config `null` entries are
/// dropped; `name` and `strategy` are injected; each referenced secret becomes
/// a managed-secret reference resolved on demand by `OpCtx::resolve_secrets`.
auto build_platform_auth_record(std::string_view name,
                                platform_authentication auth) -> record;

} // namespace tenzir
