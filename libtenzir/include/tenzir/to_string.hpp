//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/multi_series.hpp>

namespace tenzir {

/// Converts the given series into strings, using TQL-style printing.
auto to_string(multi_series ms, location loc, diagnostic_handler& dh)
  -> basic_series<string_type>;

} // namespace tenzir
