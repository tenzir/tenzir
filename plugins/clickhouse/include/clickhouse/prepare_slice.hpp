//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
#pragma once

#include "clickhouse/transformers.hpp"

namespace tenzir::plugins::clickhouse {

auto prepare_slice(table_slice const& slice, transformer_record const& tr,
                   diagnostic_handler& dh, location operator_location)
  -> table_slice;

// Extract mapped paths and pack the remaining record into the catch-all.
// Value validation and JSON serialization belong to the existing writer.
auto restructure_for_catch_all(table_slice const& slice,
                               transformer_record const& tr) -> table_slice;

} // namespace tenzir::plugins::clickhouse
