// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/diagnostics.hpp"
#include "tenzir/location.hpp"
#include "tenzir/nova/array.hpp"

namespace tenzir::nova {

struct CensorResult {
  Array<Data> data;
  /// Whether any secret was replaced.
  bool censored = false;
};

/// Replaces every secret, including nested ones, with the string `"***"`,
/// preserving null masks and record shapes. Returns the input unchanged if it
/// contains no secrets.
[[nodiscard]] auto censor_secrets(Array<Data> values) -> CensorResult;

/// Warns that secrets in the value of the expression at `source` were
/// censored instead of being stored in events.
auto warn_censored_secrets(location source, diagnostic_handler& dh) -> void;

} // namespace tenzir::nova
