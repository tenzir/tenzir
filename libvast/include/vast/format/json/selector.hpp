//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/error.hpp"
#include "vast/module.hpp"

#include <optional>
#include <simdjson.h>

namespace vast::format::json {

struct selector {
  virtual ~selector() noexcept = default;

  /// Locates the type for a given JSON object.
  [[nodiscard]] virtual std::optional<type>
  operator()(const ::simdjson::dom::object& obj) const = 0;

  /// Sets the module.
  [[nodiscard]] virtual caf::error module(const vast::module& mod) = 0;

  /// Retrieves the current module.
  [[nodiscard]] virtual vast::module module() const = 0;
};

} // namespace vast::format::json
