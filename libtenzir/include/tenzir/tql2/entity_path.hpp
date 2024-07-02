//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/debug_writer.hpp"

#include <span>

namespace tenzir {

class entity_path {
public:
  entity_path() = default;

  explicit entity_path(std::vector<std::string> path)
    : segments_{std::move(path)} {
  }

  auto resolved() const -> bool {
    return not segments_.empty();
  }

  auto segments() const -> std::span<const std::string> {
    return segments_;
  }

  friend auto inspect(auto& f, entity_path& x) -> bool {
    if (auto dbg = as_debug_writer(f)) {
      if (not x.resolved()) {
        return dbg->fmt_value("unresolved");
      }
      return dbg->fmt_value("#{}", fmt::join(x.segments_, "::"));
    }
    return f.apply(x.segments_);
  }

private:
  // TODO: Storing resolved entities as strings is quite inefficient.
  std::vector<std::string> segments_;
};

} // namespace tenzir
