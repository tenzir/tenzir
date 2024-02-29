//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/debug_writer.hpp"

#include <limits>

namespace tenzir::tql2 {

struct entity_id {
  size_t id = std::numeric_limits<size_t>::max();

  auto resolved() const -> bool {
    return id != std::numeric_limits<size_t>::max();
  }

  auto debug_inspect(debug_writer& dbg) const -> bool;

  friend auto inspect(auto& f, entity_id& x) -> bool {
    if (auto dbg = as_debug_writer(f)) {
      return x.debug_inspect(*dbg);
    }
    return f.apply(x.id);
  }
};

} // namespace tenzir::tql2
