//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/diagnostics.hpp>
#include <tenzir/option.hpp>

#include <string_view>

namespace tenzir::plugins::clickhouse {

/// Builds a query diagnostic, redacting tokens and adding backend-specific hints.
auto query_error(bool hydrolix, std::string_view token, bool tls_enabled,
                 Option<int> code, std::string_view raw_message)
  -> diagnostic_builder;

} // namespace tenzir::plugins::clickhouse
