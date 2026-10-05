//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/diagnostics.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/option.hpp"

#include <clickhouse/block.h>

#include <string_view>

namespace tenzir::plugins::clickhouse {

/// Decodes `block` column by column into events named `schema_name`.
///
/// Columns whose type does not decode (see `classify`) are dropped with a
/// warning. Values that TQL cannot represent, such as dates past 2262, become
/// `null` with one warning per column. Returns `None` for an empty block and
/// for a block without any decodable column.
auto block_to_events(::clickhouse::Block const& block,
                     std::string_view schema_name, diagnostic_handler& dh)
  -> Option<nova::Events>;

} // namespace tenzir::plugins::clickhouse
