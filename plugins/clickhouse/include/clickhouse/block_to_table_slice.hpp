//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/diagnostics.hpp"
#include "tenzir/option.hpp"

#include <clickhouse/block.h>

#include <string_view>

namespace tenzir::plugins::clickhouse {

auto block_to_table_slice(::clickhouse::Block const& block,
                          std::string_view schema_name,
                          std::string_view service, diagnostic_handler& dh)
  -> Option<table_slice>;

} // namespace tenzir::plugins::clickhouse
