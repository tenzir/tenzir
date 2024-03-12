//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/debug_writer.hpp"

namespace tenzir::tql2 {

class entity_path {
public:
  entity_path() = default;

  explicit entity_path(std::vector<std::string> path) : path_{std::move(path)} {
  }

  auto resolved() const -> bool {
    return not path_.empty();
  }

  friend auto inspect(auto& f, entity_path& x) -> bool {
    if (auto dbg = as_debug_writer(x)) {
      if (not x.resolved()) {
        return dbg->fmt_value("unresolved");
      }
      return dbg->fmt_value("{}", fmt::join(x.path_, "::"));
    }
    return f.apply(x.path_);
  }

private:
  // TODO: Storing resolved entities as strings is quite inefficient.
  std::vector<std::string> path_;
};

} // namespace tenzir::tql2
