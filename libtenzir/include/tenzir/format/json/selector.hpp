//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/error.hpp"
#include "tenzir/module.hpp"

#include <optional>
#include <simdjson.h>

namespace tenzir::format::json {

struct selector {
  virtual ~selector() noexcept = default;

  /// Locates the type for a given JSON object.
  [[nodiscard]] virtual std::optional<type>
  operator()(const ::simdjson::dom::object& obj) const = 0;

  /// Sets the module.
  [[nodiscard]] virtual caf::error module(const tenzir::module& mod) = 0;

  /// Retrieves the current module.
  [[nodiscard]] virtual tenzir::module module() const = 0;
};

} // namespace tenzir::format::json
