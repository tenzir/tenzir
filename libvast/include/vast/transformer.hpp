//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/table_slice.hpp"

#include <memory>
#include <variant>

namespace vast {

using dynamic_input
  = std::variant<std::monostate, generator<table_slice>, generator<chunk_ptr>>;

using dynamic_output
  = std::variant<generator<std::monostate>, generator<table_slice>,
                 generator<chunk_ptr>>;

class transformer_control {
public:
  virtual ~transformer_control() = default;

  virtual void abort(caf::error error) = 0;
};

class transformer {
public:
  virtual ~transformer() = default;

  virtual auto
  instantiate(dynamic_input input, transformer_control& control) const
    -> caf::expected<dynamic_output>
    = 0;

  virtual auto clone() const -> std::unique_ptr<transformer> {
    die("not implemented");
  }
};

using transformer_ptr = std::unique_ptr<transformer>;

} // namespace vast
