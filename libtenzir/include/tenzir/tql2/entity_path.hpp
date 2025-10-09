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

#include <string>

namespace tenzir {

/// The package namespace in which an entity is looked up.
///
/// Historically this was a closed enum with values 'std' and 'cfg'. We now use
/// strings to allow arbitrary package roots (e.g., packages::<id>).
using entity_pkg = std::string;
inline constexpr std::string_view entity_pkg_std = "std";
inline constexpr std::string_view entity_pkg_cfg = "cfg";

/// Models the available entity namespaces.
TENZIR_ENUM(entity_ns, op, fn, mod);

/// Every entity is identified by a combination of three things:
/// - The package where the lookup is started.
/// - The path within that package that leads the entity.
/// - The namespace of the entity, because the same name can be used multiple
///   times in different namespaces.
class entity_path {
public:
  entity_path() = default;

  entity_path(entity_pkg pkg, std::vector<std::string> segments, entity_ns ns)
    : pkg_{pkg}, segments_{std::move(segments)}, ns_{ns} {
    TENZIR_ASSERT(resolved());
  }

  auto resolved() const -> bool {
    return not segments_.empty();
  }

  auto pkg() const -> const entity_pkg& {
    TENZIR_ASSERT(resolved());
    return pkg_;
  }

  auto segments() const -> std::span<const std::string> {
    TENZIR_ASSERT(resolved());
    return segments_;
  }

  auto ns() const -> entity_ns {
    TENZIR_ASSERT(resolved());
    return ns_;
  }

  friend auto inspect(auto& f, entity_path& x) -> bool {
    if (auto dbg = as_debug_writer(f)) {
      if (not x.resolved()) {
        return dbg->fmt_value("unresolved");
      }
      return dbg->fmt_value("{}::{}/{}", x.pkg_, fmt::join(x.segments_, "::"),
                            x.ns_);
    }
    return f.object(x).fields(f.field("pkg", x.pkg_),
                              f.field("segments", x.segments_),
                              f.field("ns", x.ns_));
  }

private:
  entity_pkg pkg_{};
  std::vector<std::string> segments_;
  entity_ns ns_{};
};

} // namespace tenzir
