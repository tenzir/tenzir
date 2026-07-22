//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/diagnostics.hpp"
#include "tenzir/pipeline.hpp"

namespace tenzir::tql {

/// Create a parser interface over `source` for parsing operator and connector
/// arguments. No locations will be set.
///
/// This is part of the retained TQL1 argument-parser stack used by the
/// old-executor `parse(parser_interface&)` plugin ABI. It does not parse
/// pipelines.
auto make_parser_interface(std::string source, diagnostic_handler& diag)
  -> std::unique_ptr<parser_interface>;

} // namespace tenzir::tql
