//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include <fmt/format.h>

#include <optional>
#include <string>
#include <string_view>

namespace tenzir {

auto get_trace_id(std::string_view data) -> std::optional<std::string>;

template <class... Ts>
auto trace(std::optional<std::string> id, Ts&&... ts) {
  if (!id) {
    return;
  }
  TENZIR_WARN("{}",
              *id + ((", " + fmt::to_string(std::forward<Ts>(ts))) + ...));
}

} // namespace tenzir
