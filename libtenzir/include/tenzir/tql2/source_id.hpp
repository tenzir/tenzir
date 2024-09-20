//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

namespace tenzir {

class source_id {
public:
  source_id() : value{0} {
  }

  auto operator<=>(const source_id&) const = default;

  static const source_id unknown;

  // TODO: We need this. But the source id is only valid in the current context.
  // So what do we do?
  friend auto inspect(auto& f, source_id& x) -> bool {
    return f.apply(x.value);
  }

private:
  friend class source_map;
  friend struct std::hash<tenzir::source_id>;

  explicit source_id(uint32_t id) : value{id} {
  }

  uint32_t value;
};

inline source_id const source_id::unknown = source_id{};

} // namespace tenzir

template <>
struct std::hash<tenzir::source_id> {
  auto operator()(const tenzir::source_id& x) const -> size_t {
    return x.value;
  }
};
