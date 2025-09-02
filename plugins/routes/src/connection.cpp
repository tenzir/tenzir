//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors

#include "routes/connection.hpp"

#include <tenzir/diagnostics.hpp>
#include <tenzir/view.hpp>

namespace tenzir::plugins::routes {

auto connection::make(const view<record>& data, session ctx) -> failure_or<connection> {
  auto result = connection{};
  auto has_errors = false;
  for (const auto& [key, value] : data) {
    if (key == "from") {
      const auto* from_str = try_as<std::string_view>(&value);
      if (not from_str) {
        diagnostic::error("from must be a string")
          .note("invalid connection definition")
          .emit(ctx);
        has_errors = true;
        continue;
      }
      result.from = std::string{*from_str};
      continue;
    }
    if (key == "to") {
      const auto* to_str = try_as<std::string_view>(&value);
      if (not to_str) {
        diagnostic::error("to must be a string")
          .note("invalid connection definition")
          .emit(ctx);
        has_errors = true;
        continue;
      }
      result.to = std::string{*to_str};
      continue;
    }
    diagnostic::error("unknown field '{}'", key)
      .note("valid fields are: 'from', 'to'")
      .note("invalid connection definition")
      .emit(ctx);
    has_errors = true;
  }
  if (result.from.empty()) {
    diagnostic::error("missing required field 'from'")
      .note("invalid connection definition")
      .emit(ctx);
    has_errors = true;
  }
  if (result.to.empty()) {
    diagnostic::error("missing required field 'to'")
      .note("invalid connection definition")
      .emit(ctx);
    has_errors = true;
  }
  if (has_errors) {
    return failure::promise();
  }
  return result;
}

auto connection::to_record() const -> record {
  auto result = record{};
  result["from"] = from;
  result["to"] = to;
  return result;
}

} // namespace tenzir::plugins::routes
