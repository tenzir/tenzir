//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tracing.hpp"

namespace tenzir {

auto get_trace_id(std::string_view data) -> std::optional<std::string> {
  auto trace_indicator = data.find("// TRACE");
  return trace_indicator == std::string::npos
           ? std::nullopt
           : std::optional{std::string{data.substr(
             trace_indicator,
             data.find_first_of("\\\"\n", trace_indicator) - trace_indicator)}};
}

} // namespace tenzir
