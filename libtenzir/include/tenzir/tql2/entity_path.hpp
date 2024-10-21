//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/debug_writer.hpp"
#include "tenzir/detail/enum.hpp"

namespace tenzir {

TENZIR_ENUM(
  /// Models the available entity namespaces.
  entity_ns,
  /// The operator namespace contains only operators.
  op,
  /// The function namespace contains both functions and methods.
  fn);

class entity_path {
public:
  entity_path() = default;

  explicit entity_path(std::vector<std::string> segments, entity_ns ns)
    : segments_{std::move(segments)}, ns_{ns} {
    TENZIR_ASSERT(resolved());
  }

  auto resolved() const -> bool {
    return not segments_.empty();
  }

  auto ns() const -> entity_ns {
    TENZIR_ASSERT(resolved());
    return ns_;
  }

  auto segments() const -> std::span<const std::string> {
    TENZIR_ASSERT(resolved());
    return segments_;
  }

  friend auto inspect(auto& f, entity_path& x) -> bool {
    if (auto dbg = as_debug_writer(f)) {
      if (not x.resolved()) {
        return dbg->fmt_value("unresolved");
      }
      return dbg->fmt_value("{}/{}", fmt::join(x.segments_, "::"), x.ns_);
    }
    return f.object(x).fields(f.field("path", x.segments_),
                              f.field("ns", x.ns_));
  }

private:
  std::vector<std::string> segments_;
  entity_ns ns_{};
};

} // namespace tenzir
