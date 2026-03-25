//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/multi_series.hpp>
#include <tenzir/view3.hpp>

namespace tenzir {

auto to_string(data_view3 v, location loc, diagnostic_handler& dh)
  -> std::optional<std::string>;

/// Converts the given series into strings, using TQL-style printing.
auto to_string(multi_series ms, location loc, diagnostic_handler& dh)
  -> basic_series<string_type>;

} // namespace tenzir
