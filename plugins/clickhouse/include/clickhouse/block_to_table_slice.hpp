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

namespace tenzir::plugins::clickhouse {

auto block_to_table_slice(::clickhouse::Block const& block,
                          std::string_view schema_name, diagnostic_handler& dh)
  -> Option<table_slice>;

/// Returns whether `block_to_table_slice` decodes a column of the ClickHouse
/// type `type`, spelled as `DESCRIBE TABLE` renders it. A column of a type
/// that does not decode is absent from the operator's output, as is a tuple
/// with any element of such a type.
auto is_decodable_type(std::string_view type) -> bool;

} // namespace tenzir::plugins::clickhouse
